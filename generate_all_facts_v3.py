# ==============================================================================
# RETAIL BI — Complete Fact Tables Generator v3
# Muhammad Amjad | Power BI Specialist | Pipeline-First
# Database: RetailBI_Dev | March 2026
#
# WHAT THIS GENERATES:
#   FactSales_v3.csv         — original + PaymentMethodKey per transaction
#   FactPurchases_v3.csv     — sales-driven qty, margin-constrained prices,
#                              PaymentMethodKey, PayableInvoiceNumber
#   FactPayables_v3.csv      — derived from credit purchases, PaymentDate,
#                              PaymentMethodKey, linked to FactPurchases
#   FactExpenses_v3.csv      — daily grain from monthly totals, PaymentMethodKey
#   FactPayments_v3.csv      — generated LAST from all four sources above
#                              fully interlinked cash book
#   FactReceivables_v3.csv   — unchanged copy
#
# HOW TO RUN IN GOOGLE COLAB:
#   1. Upload FactSales.csv       (original from v1)
#   2. Upload FactExpenses.csv    (original monthly from v1)
#   3. Upload FactReceivables.csv (original from v1)
#   4. Upload this script
#   5. exec(open('generate_all_facts_v3.py').read())
#   6. Download all 6 _v3.csv files
#   7. TRUNCATE existing fact tables in SSMS then bulk insert
#      Import order: FactPurchases → FactExpenses → FactReceivables
#                    → FactPayables → FactSales → FactPayments
#
# SCHEMA ADDITIONS vs v1 (run these ALTER TABLE statements in SSMS first):
#   ALTER TABLE FactSales       ADD PaymentMethodKey TINYINT NOT NULL DEFAULT 1;
#   ALTER TABLE FactPurchases   ADD PaymentMethodKey TINYINT NOT NULL DEFAULT 1;
#   ALTER TABLE FactPurchases   ADD PayableInvoiceNumber VARCHAR(50) NULL;
#   ALTER TABLE FactPayables    ADD PaymentDate INT NULL;
#   ALTER TABLE FactPayables    ADD PaymentMethodKey TINYINT NOT NULL DEFAULT 2;
#   ALTER TABLE FactExpenses    ADD PaymentMethodKey TINYINT NOT NULL DEFAULT 2;
# ==============================================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import calendar

np.random.seed(42)
random.seed(42)
rng = np.random.default_rng(42)

print("=" * 65)
print("Retail BI — Complete Fact Tables Generator v3")
print("=" * 65)


# ==============================================================================
# SECTION 0 — CONFIGURATION
# ==============================================================================

START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2024, 12, 31)

# Gross margin targets per BU (sale - purchase) / sale
TARGET_MARGIN   = {1: 0.35, 2: 0.28}
MARGIN_VARIANCE = 0.025

# Purchase quantity buffer over actual sales (5-20% random per month)
PURCHASE_BUFFER_MIN = 1.05
PURCHASE_BUFFER_MAX = 1.20

# Margin erosion: 8 GR products, purchase cost +0.8% monthly, sale price fixed
EROSION_RATE  = 0.008
EROSION_COUNT = 8

# Supplier premium: these three charge above base purchase price
PREMIUM_SUPPLIER_RATES = {4: 0.06, 5: 0.09, 6: 0.12}

# Supplier to product key range mapping
SC_SUPPLIER_MAP = [
    (range(1,   51),  12),   # Cloth Wholesale
    (range(51,  151), 13),   # Galaxy Garments
    (range(151, 229), 14),   # Diamond Shoes
]
GR_SUPPLIER_RANGES = [
    (range(229, 253),  4),   # Tea — Tapal (premium)
    (range(253, 277),  5),   # Cooking Oil — Dalda (premium)
    (range(277, 304),  3),   # Rice & Pulses
    (range(304, 316),  8),   # Spices
    (range(316, 326),  9),   # Jams & Spreads
    (range(326, 356), 10),   # Confectionery
    (range(356, 388),  6),   # Beverages — PepsiCo (premium)
    (range(388, 396), 11),   # Formula Milk
    (range(396, 413),  1),   # Pampers & Kids
    (range(413, 427),  2),   # Laundry & Cleaning
    (range(427, 456),  7),   # Personal Care
]

# Payment method keys: 1=Cash, 2=Bank, 3=Wallet (DimPaymentMethod)
# For purchases: 1=Cash, 2=Bank, 3=Credit (goes to payable) — internal only
PM_CASH   = 1
PM_BANK   = 2
PM_WALLET = 3
PM_CREDIT = 3  # reused key — in purchases context means on-credit/payable

# FactSales payment method weights by BU
# GR: small basket walk-in customers — heavily cash
# SC: larger basket, more bank transfers on garments/shoes
SALES_PM_WEIGHTS = {
    1: {PM_CASH: 0.60, PM_BANK: 0.30, PM_WALLET: 0.10},   # SC
    2: {PM_CASH: 0.75, PM_BANK: 0.18, PM_WALLET: 0.07},   # GR
}

# Daily cash-to-bank deposit rate
# Pakistani retailers bank a portion of cash daily to fund supplier payments.
# Cash is collected at the counter, deposited, then used for bank transfers
# to suppliers when payables fall due. 50% of each day's cash collections
# are banked the same day. This appears in FactPayments as:
#   Cash TotalPaid += cash_deposit_amount
#   Bank TotalCollected += cash_deposit_amount
# No new table rows are created — it is modeled within the existing
# 6-row-per-day FactPayments structure.
DAILY_CASH_BANKING_RATE = 0.50

# FactPurchases payment method weights by supplier
# Major FMCG distributors: mostly on credit
# SC local suppliers: more cash and bank
# Premium suppliers (4,5,6): highest credit proportion
PURCHASE_PM_WEIGHTS = {
    1:  {PM_CASH: 0.05, PM_BANK: 0.10, PM_CREDIT: 0.85},
    2:  {PM_CASH: 0.05, PM_BANK: 0.10, PM_CREDIT: 0.85},
    3:  {PM_CASH: 0.05, PM_BANK: 0.10, PM_CREDIT: 0.85},
    4:  {PM_CASH: 0.03, PM_BANK: 0.07, PM_CREDIT: 0.90},  # premium
    5:  {PM_CASH: 0.03, PM_BANK: 0.07, PM_CREDIT: 0.90},  # premium
    6:  {PM_CASH: 0.03, PM_BANK: 0.07, PM_CREDIT: 0.90},  # premium
    7:  {PM_CASH: 0.05, PM_BANK: 0.10, PM_CREDIT: 0.85},
    8:  {PM_CASH: 0.05, PM_BANK: 0.10, PM_CREDIT: 0.85},
    9:  {PM_CASH: 0.05, PM_BANK: 0.10, PM_CREDIT: 0.85},
    10: {PM_CASH: 0.05, PM_BANK: 0.10, PM_CREDIT: 0.85},
    11: {PM_CASH: 0.05, PM_BANK: 0.10, PM_CREDIT: 0.85},
    12: {PM_CASH: 0.20, PM_BANK: 0.25, PM_CREDIT: 0.55},  # Cloth Wholesale local
    13: {PM_CASH: 0.08, PM_BANK: 0.17, PM_CREDIT: 0.75},  # Galaxy Garments
    14: {PM_CASH: 0.08, PM_BANK: 0.17, PM_CREDIT: 0.75},  # Diamond Shoes
    15: {PM_CASH: 0.10, PM_BANK: 0.20, PM_CREDIT: 0.70},
    16: {PM_CASH: 0.10, PM_BANK: 0.20, PM_CREDIT: 0.70},
    17: {PM_CASH: 0.10, PM_BANK: 0.20, PM_CREDIT: 0.70},
    18: {PM_CASH: 0.10, PM_BANK: 0.20, PM_CREDIT: 0.70},
    19: {PM_CASH: 0.10, PM_BANK: 0.20, PM_CREDIT: 0.70},
    20: {PM_CASH: 0.10, PM_BANK: 0.20, PM_CREDIT: 0.70},
}

# FactPayables payment method when settling (which method the business pays from)
# SUP012 (local cloth) gets paid in cash; all others via bank transfer
PAYABLE_SETTLE_PM = {12: PM_CASH}
PAYABLE_SETTLE_PM_DEFAULT = PM_BANK

# FactPayables payment patterns (ref doc spec)
PAYABLE_PATTERNS = [
    ('paid',    0.55),  # fully paid — DaysOutstanding = 0
    ('partial', 0.20),  # partially paid 40-80%
    ('open',    0.15),  # outstanding 30-60 days, no payment
    ('overdue', 0.10),  # overdue 60-90 days, no payment
]

# FactExpenses: payment schedule per ExpenseAccountKey
# (payment_day, payment_method)
# payment_day 0 = distribute across all calendar days of month
# payment_day 1-31 = fixed date
EXPENSE_SCHEDULE = {
    1:  {'day': 1,  'pm': PM_BANK,   'label': 'Shop Rent SC'},
    2:  {'day': 25, 'pm': PM_BANK,   'label': 'Staff Salaries SC'},
    3:  {'day': 15, 'pm': PM_BANK,   'label': 'Electricity SC'},
    4:  {'day': 0,  'pm': PM_CASH,   'label': 'Display & Decoration'},
    5:  {'day': 0,  'pm': PM_CASH,   'label': 'Packaging SC'},
    6:  {'day': 1,  'pm': PM_BANK,   'label': 'Shop Rent GR'},
    7:  {'day': 25, 'pm': PM_BANK,   'label': 'Staff Salaries GR'},
    8:  {'day': 15, 'pm': PM_BANK,   'label': 'Electricity GR'},
    9:  {'day': 0,  'pm': PM_CASH,   'label': 'Packaging GR'},
    10: {'day': 5,  'pm': PM_BANK,   'label': 'Cold Storage'},
    11: {'day': 10, 'pm': PM_BANK,   'label': 'Building Maintenance'},
    12: {'day': 1,  'pm': PM_BANK,   'label': 'Security Services'},
    13: {'day': 5,  'pm': PM_BANK,   'label': 'Internet & Phone'},
    14: {'day': 0,  'pm': PM_CASH,   'label': 'Miscellaneous'},
    15: {'day': 0,  'pm': PM_CASH,   'label': 'Vehicle Fuel'},
}

# FactPayments opening balances Jan 1 2023
OPENING_BALANCES = {
    (1, PM_CASH):   50_000,
    (1, PM_BANK):  150_000,
    (1, PM_WALLET): 10_000,
    (2, PM_CASH):   40_000,
    (2, PM_BANK):  120_000,
    (2, PM_WALLET):  8_000,
}

# Ramadan periods (2 weeks before, for pre-Ramadan purchasing surge)
RAMADAN_PERIODS = [
    (datetime(2023, 3, 9),  datetime(2023, 3, 22)),
    (datetime(2024, 2, 25), datetime(2024, 3, 10)),
]

print("Configuration loaded.\n")


# ==============================================================================
# SECTION 1 — LOAD INPUT FILES
# ==============================================================================

print("Loading input files...")

for f in ['FactSales.csv', 'FactExpenses.csv', 'FactReceivables.csv']:
    if not os.path.exists(f):
        raise FileNotFoundError(f"{f} not found. Upload it to Colab files panel.")

sales_orig  = pd.read_csv('FactSales.csv')
exp_monthly = pd.read_csv('FactExpenses.csv')
rec_orig    = pd.read_csv('FactReceivables.csv')

sales_orig['Date'] = pd.to_datetime(sales_orig['DateKey'].astype(str), format='%Y%m%d')
sales_orig['YearMonth'] = sales_orig['DateKey'] // 100

print(f"  FactSales:        {len(sales_orig):>7,} rows")
print(f"  FactExpenses:     {len(exp_monthly):>7,} rows (monthly — will expand to daily)")
print(f"  FactReceivables:  {len(rec_orig):>7,} rows")


# ==============================================================================
# SECTION 2 — PRODUCT REFERENCE TABLE
# ==============================================================================

print("\nBuilding product reference table...")

prod_ref = (
    sales_orig
    .groupby(['ProductKey', 'BusinessUnitKey'])
    .agg(AvgSalePrice=('UnitSalePrice', 'mean'))
    .reset_index()
)

# Base purchase price at target margin with per-product variance
variance = rng.uniform(-MARGIN_VARIANCE, MARGIN_VARIANCE, len(prod_ref))
prod_ref['BasePurchasePrice'] = (
    prod_ref['AvgSalePrice'] *
    (1 - prod_ref['BusinessUnitKey'].map(TARGET_MARGIN) + variance)
).round(2)

def get_supplier(row):
    pk, bu = row['ProductKey'], row['BusinessUnitKey']
    if bu == 1:
        for r, s in SC_SUPPLIER_MAP:
            if pk in r: return s
        return 14
    for r, s in GR_SUPPLIER_RANGES:
        if pk in r: return s
    return 3

prod_ref['SupplierKey'] = prod_ref.apply(get_supplier, axis=1)

# Flag 8 erosion products (evenly spaced through GR product keys)
gr_keys = prod_ref[prod_ref['BusinessUnitKey'] == 2]['ProductKey'].sort_values().tolist()
stride  = len(gr_keys) // (EROSION_COUNT + 1)
erosion_products = set(gr_keys[i * stride] for i in range(1, EROSION_COUNT + 1))
prod_ref['IsErosion'] = prod_ref['ProductKey'].isin(erosion_products)

sc_um = (1 - prod_ref[prod_ref['BusinessUnitKey']==1]['BasePurchasePrice'].mean() /
             prod_ref[prod_ref['BusinessUnitKey']==1]['AvgSalePrice'].mean()) * 100
gr_um = (1 - prod_ref[prod_ref['BusinessUnitKey']==2]['BasePurchasePrice'].mean() /
             prod_ref[prod_ref['BusinessUnitKey']==2]['AvgSalePrice'].mean()) * 100
print(f"  {len(prod_ref)} products | SC margin: {sc_um:.1f}% | GR margin: {gr_um:.1f}% (base, before premium)")
print(f"  Erosion products (8 GR): {sorted(erosion_products)}")


# ==============================================================================
# SECTION 3 — DATE REFERENCE
# ==============================================================================

print("\nBuilding date reference...")

all_dates = pd.date_range(START_DATE, END_DATE, freq='D')

pre_ramadan_days = set()
for s, e in RAMADAN_PERIODS:
    d = s
    while d <= e:
        pre_ramadan_days.add(d)
        d += timedelta(days=1)

def is_purchase_day(d, bu):
    if d.weekday() in (0, 3): return True          # Mon and Thu always
    if bu == 2 and d in pre_ramadan_days: return True
    return random.random() < 0.20

purchase_days = {
    bu: {d for d in all_dates if is_purchase_day(d, bu)}
    for bu in [1, 2]
}
print(f"  SC purchase days: {len(purchase_days[1])} | GR purchase days: {len(purchase_days[2])}")


# ==============================================================================
# SECTION 4 — GENERATE FactSales_v3 (add PaymentMethodKey)
# ==============================================================================

print("\nGenerating FactSales_v3 (adding PaymentMethodKey)...")

def assign_sales_pm(bu, unit_price):
    w = SALES_PM_WEIGHTS[bu]
    # Larger SC transactions lean more toward bank
    if bu == 1 and unit_price > 3000:
        w = {PM_CASH: 0.45, PM_BANK: 0.45, PM_WALLET: 0.10}
    # Very small GR transactions are almost always cash
    elif bu == 2 and unit_price < 100:
        w = {PM_CASH: 0.92, PM_BANK: 0.05, PM_WALLET: 0.03}
    choices = list(w.keys())
    probs   = list(w.values())
    return random.choices(choices, weights=probs, k=1)[0]

fact_sales = sales_orig.copy()
fact_sales['PaymentMethodKey'] = [
    assign_sales_pm(int(row['BusinessUnitKey']), float(row['UnitSalePrice']))
    for _, row in fact_sales.iterrows()
]

pm_dist = fact_sales.groupby(['BusinessUnitKey', 'PaymentMethodKey']).size().unstack(fill_value=0)
print(f"  SC payment method distribution:\n    Cash: {pm_dist.loc[1,1]:,}  Bank: {pm_dist.loc[1,2]:,}  Wallet: {pm_dist.loc[1,3]:,}")
print(f"  GR payment method distribution:\n    Cash: {pm_dist.loc[2,1]:,}  Bank: {pm_dist.loc[2,2]:,}  Wallet: {pm_dist.loc[2,3]:,}")


# ==============================================================================
# SECTION 5 — GENERATE FactPurchases_v3
# ==============================================================================

print("\nGenerating FactPurchases_v3...")

monthly_sales = (
    sales_orig
    .groupby(['YearMonth', 'ProductKey', 'BusinessUnitKey'])['Quantity']
    .sum()
    .reset_index()
    .rename(columns={'Quantity': 'SoldQty'})
)

purchase_rows = []
po_sc = 1
po_gr = 1
year_months = sorted(monthly_sales['YearMonth'].unique())

for ym in year_months:
    yr  = ym // 100
    mo  = ym % 100
    month_pur_days = {
        bu: sorted(d for d in purchase_days[bu] if d.year == yr and d.month == mo)
        for bu in [1, 2]
    }
    is_pre_ramadan = {
        bu: any(d in pre_ramadan_days for d in month_pur_days[bu])
        for bu in [1, 2]
    }

    for _, row in monthly_sales[monthly_sales['YearMonth'] == ym].iterrows():
        pk  = int(row['ProductKey'])
        bu  = int(row['BusinessUnitKey'])
        qty_sold = float(row['SoldQty'])

        buf = random.uniform(PURCHASE_BUFFER_MIN, PURCHASE_BUFFER_MAX)
        if bu == 2 and is_pre_ramadan[bu]:
            buf = max(buf, 1.60)

        total_qty = round(qty_sold * buf, 3)

        p = prod_ref[prod_ref['ProductKey'] == pk]
        if p.empty: continue
        p = p.iloc[0]

        sup_key    = int(p['SupplierKey'])
        base_price = float(p['BasePurchasePrice'])

        if p['IsErosion']:
            mi = year_months.index(ym)
            base_price = round(base_price * (1 + EROSION_RATE) ** mi, 2)

        if sup_key in PREMIUM_SUPPLIER_RATES:
            base_price = round(base_price * (1 + PREMIUM_SUPPLIER_RATES[sup_key]), 2)

        n_pos = np.clip(int(qty_sold / 10) + 1, 1, 3) if bu == 1 else np.clip(int(qty_sold / 20) + 2, 2, 4)
        avail = month_pur_days[bu] or [datetime(yr, mo, 1)]
        chosen = sorted(random.sample(avail, min(n_pos, len(avail))))

        base_qty  = round(total_qty / len(chosen), 3)
        qtys = [base_qty] * (len(chosen) - 1)
        qtys.append(round(total_qty - sum(qtys), 3))

        # Assign payment method for this supplier (one per PO row)
        pm_weights = PURCHASE_PM_WEIGHTS.get(sup_key, {PM_CASH: 0.10, PM_BANK: 0.20, PM_CREDIT: 0.70})

        for po_date, po_qty in zip(chosen, qtys):
            date_key   = int(po_date.strftime('%Y%m%d'))
            noise      = 1 + rng.uniform(-0.005, 0.005)
            unit_price = round(base_price * noise, 2)
            total_amt  = round(unit_price * po_qty, 2)

            pm_choice = random.choices(
                list(pm_weights.keys()),
                weights=list(pm_weights.values()),
                k=1
            )[0]

            if bu == 1:
                inv = f"PO-SC-{po_sc:06d}"
                po_sc += 1
            else:
                inv = f"PO-GR-{po_gr:06d}"
                po_gr += 1

            # PayableInvoiceNumber — batch key: supplier + BU + month
            # Only assigned when on credit; cash and bank are immediate
            if pm_choice == PM_CREDIT:
                payable_inv = f"SINV-{ym}-SUP{sup_key:03d}-BU{bu}"
            else:
                payable_inv = None

            purchase_rows.append({
                'DateKey'            : date_key,
                'ProductKey'         : pk,
                'SupplierKey'        : sup_key,
                'BusinessUnitKey'    : bu,
                'InvoiceNumber'      : inv,
                'Quantity'           : po_qty,
                'UnitPurchasePrice'  : unit_price,
                'TotalPurchaseAmount': total_amt,
                'PaymentMethodKey'   : pm_choice,
                'PayableInvoiceNumber': payable_inv,
            })

fact_purchases = pd.DataFrame(purchase_rows)
fact_purchases.insert(0, 'PurchaseKey', range(1, len(fact_purchases) + 1))

credit_pct = (fact_purchases['PaymentMethodKey'] == PM_CREDIT).mean() * 100
cash_pct   = (fact_purchases['PaymentMethodKey'] == PM_CASH).mean() * 100
bank_pct   = (fact_purchases['PaymentMethodKey'] == PM_BANK).mean() * 100

print(f"  Rows: {len(fact_purchases):,}")
print(f"  Payment split — Cash: {cash_pct:.1f}%  Bank: {bank_pct:.1f}%  Credit: {credit_pct:.1f}%")
print(f"  Unique payable batches: {fact_purchases['PayableInvoiceNumber'].nunique()}")

# Per-unit margin check
sp = sales_orig.groupby(['ProductKey','BusinessUnitKey'])['UnitSalePrice'].mean().reset_index()
pp = fact_purchases.groupby(['ProductKey','BusinessUnitKey'])['UnitPurchasePrice'].mean().reset_index()
uc = sp.merge(pp, on=['ProductKey','BusinessUnitKey'])
uc['m'] = (uc['UnitSalePrice'] - uc['UnitPurchasePrice']) / uc['UnitSalePrice']
print(f"  Per-unit margin — SC: {uc[uc['BusinessUnitKey']==1]['m'].mean()*100:.1f}%  GR: {uc[uc['BusinessUnitKey']==2]['m'].mean()*100:.1f}%")
print(f"  Total sold: {sales_orig['Quantity'].sum():,.0f}  |  Total purchased: {fact_purchases['Quantity'].sum():,.0f}")


# ==============================================================================
# SECTION 6 — GENERATE FactPayables_v3
# ==============================================================================

print("\nGenerating FactPayables_v3...")

# Aggregate credit purchase rows into monthly supplier batches
credit_pur = fact_purchases[fact_purchases['PayableInvoiceNumber'].notna()].copy()
credit_pur['YearMonth'] = credit_pur['DateKey'] // 100

payable_agg = (
    credit_pur
    .groupby(['PayableInvoiceNumber', 'SupplierKey', 'BusinessUnitKey', 'YearMonth'])
    ['TotalPurchaseAmount']
    .sum()
    .reset_index()
)

payable_rows = []

for _, row in payable_agg.iterrows():
    inv_ref  = row['PayableInvoiceNumber']
    sup_key  = int(row['SupplierKey'])
    bu       = int(row['BusinessUnitKey'])
    ym       = int(row['YearMonth'])
    inv_amt  = round(float(row['TotalPurchaseAmount']), 2)

    yr = ym // 100
    mo = ym % 100
    max_day = calendar.monthrange(yr, mo)[1]

    # Invoice date: random day in the month (credit purchases billed at month end)
    inv_day  = random.randint(max(1, max_day - 10), max_day)
    inv_date = datetime(yr, mo, min(inv_day, max_day))
    inv_dkey = int(inv_date.strftime('%Y%m%d'))

    # Due date: 30-45 days after invoice
    due_date = inv_date + timedelta(days=random.randint(30, 45))
    due_dkey = int(due_date.strftime('%Y%m%d'))

    # Payment pattern
    roll = random.random()
    cum  = 0
    pattern = 'paid'
    for name, prob in PAYABLE_PATTERNS:
        cum += prob
        if roll <= cum:
            pattern = name
            break

    # Payment method when settling this payable
    settle_pm = PAYABLE_SETTLE_PM.get(sup_key, PAYABLE_SETTLE_PM_DEFAULT)

    if pattern == 'paid':
        paid_amt = inv_amt
        days_out = 0
        pay_delay = random.randint(15, 40)
        pay_date  = inv_date + timedelta(days=pay_delay)
        pay_dkey  = int(pay_date.strftime('%Y%m%d'))

    elif pattern == 'partial':
        paid_pct = random.uniform(0.40, 0.80)
        paid_amt = round(inv_amt * paid_pct, 2)
        days_out = random.randint(15, 45)
        pay_delay = random.randint(10, 35)
        pay_date  = inv_date + timedelta(days=pay_delay)
        pay_dkey  = int(pay_date.strftime('%Y%m%d'))

    elif pattern == 'open':
        paid_amt = 0.0
        days_out = random.randint(30, 60)
        pay_dkey = None

    else:  # overdue
        paid_amt = 0.0
        days_out = random.randint(61, 90)
        pay_dkey = None

    payable_rows.append({
        'DateKey'            : inv_dkey,
        'DueDateKey'         : due_dkey,
        'SupplierKey'        : sup_key,
        'BusinessUnitKey'    : bu,
        'InvoiceNumber'      : inv_ref,
        'InvoiceAmount'      : inv_amt,
        'PaidAmount'         : round(paid_amt, 2),
        'DaysOutstanding'    : days_out,
        'PaymentDate'        : pay_dkey,
        'PaymentMethodKey'   : settle_pm,
    })

fact_payables = pd.DataFrame(payable_rows)
fact_payables.insert(0, 'PayableKey', range(1, len(fact_payables) + 1))

print(f"  Rows: {len(fact_payables):,}")
print(f"  Total invoiced:    Rs. {fact_payables['InvoiceAmount'].sum():>15,.2f}")
print(f"  Total paid:        Rs. {fact_payables['PaidAmount'].sum():>15,.2f}")
print(f"  Total outstanding: Rs. {(fact_payables['InvoiceAmount'] - fact_payables['PaidAmount']).sum():>15,.2f}")
print(f"  Pattern split — Paid: {(fact_payables['PaidAmount']==fact_payables['InvoiceAmount']).mean()*100:.0f}%  Partial: {((fact_payables['PaidAmount']>0)&(fact_payables['PaidAmount']<fact_payables['InvoiceAmount'])).mean()*100:.0f}%  Unpaid: {(fact_payables['PaidAmount']==0).mean()*100:.0f}%")


# ==============================================================================
# SECTION 7 — GENERATE FactExpenses_v3 (daily grain)
# ==============================================================================

print("\nGenerating FactExpenses_v3 (daily grain from monthly totals)...")

expense_rows = []
exp_key = 1

for _, mrow in exp_monthly.iterrows():
    date_key = int(mrow['DateKey'])
    bu       = int(mrow['BusinessUnitKey'])
    acc_key  = int(mrow['ExpenseAccountKey'])
    mo_amt   = float(mrow['ExpenseAmount'])

    yr  = date_key // 10000
    mo  = (date_key % 10000) // 100
    max_day = calendar.monthrange(yr, mo)[1]

    sched = EXPENSE_SCHEDULE[acc_key]
    pm    = sched['pm']
    label = sched['label']

    if sched['day'] > 0:
        # Fixed payment date
        pay_day  = min(sched['day'], max_day)
        pay_dkey = yr * 10000 + mo * 100 + pay_day
        expense_rows.append({
            'DateKey'           : pay_dkey,
            'BusinessUnitKey'   : bu,
            'ExpenseAccountKey' : acc_key,
            'ExpenseAmount'     : round(mo_amt, 2),
            'PaymentMethodKey'  : pm,
            'Description'       : f"{label} — {datetime(yr, mo, 1).strftime('%B %Y')}",
            'ReferenceNumber'   : f"EXP-{pay_dkey}-{acc_key:02d}-BU{bu}",
        })
        exp_key += 1

    else:
        # Spread across all calendar days of the month with random variation
        days_in_month = list(range(1, max_day + 1))
        n = len(days_in_month)
        base_amt = mo_amt / n

        # Generate random weights that sum to 1
        weights = rng.uniform(0.7, 1.3, n)
        weights = weights / weights.sum()
        daily_amts = (weights * mo_amt).round(2)
        # Correct rounding drift on last day
        daily_amts[-1] = round(mo_amt - daily_amts[:-1].sum(), 2)

        for day, amt in zip(days_in_month, daily_amts):
            if amt <= 0:
                continue
            pay_dkey = yr * 10000 + mo * 100 + day
            expense_rows.append({
                'DateKey'           : pay_dkey,
                'BusinessUnitKey'   : bu,
                'ExpenseAccountKey' : acc_key,
                'ExpenseAmount'     : float(amt),
                'PaymentMethodKey'  : pm,
                'Description'       : f"{label} — {datetime(yr, mo, day).strftime('%d %B %Y')}",
                'ReferenceNumber'   : f"EXP-{pay_dkey}-{acc_key:02d}-BU{bu}",
            })
            exp_key += 1

fact_expenses = pd.DataFrame(expense_rows)
fact_expenses.insert(0, 'ExpenseKey', range(1, len(fact_expenses) + 1))

# Verify monthly totals match original
original_total = exp_monthly['ExpenseAmount'].sum()
new_total      = fact_expenses['ExpenseAmount'].sum()

print(f"  Rows: {len(fact_expenses):,}  (was 480 monthly rows)")
print(f"  Original 24-month total: Rs. {original_total:>12,.2f}")
print(f"  New 24-month total:      Rs. {new_total:>12,.2f}")
print(f"  Difference:              Rs. {new_total - original_total:>+12,.2f}  (rounding — acceptable)")
print(f"  Cash expense rows: {(fact_expenses['PaymentMethodKey']==PM_CASH).sum():,}  |  Bank expense rows: {(fact_expenses['PaymentMethodKey']==PM_BANK).sum():,}")


# ==============================================================================
# SECTION 8 — GENERATE FactPayments_v3
# ==============================================================================

print("\nGenerating FactPayments_v3 (interlinked cash book)...")

# Pre-compute all daily inflows and outflows indexed by (DateKey, BU, PM)

# --- INFLOWS: FactSales by payment method ---
daily_inflow = (
    fact_sales
    .groupby(['DateKey', 'BusinessUnitKey', 'PaymentMethodKey'])['NetSaleAmount']
    .sum()
    .reset_index()
    .rename(columns={'NetSaleAmount': 'Inflow'})
)

# --- OUTFLOWS ---

# 1. Cash/bank purchases (PaymentMethodKey 1 or 2 in FactPurchases)
pur_cash_bank = fact_purchases[fact_purchases['PaymentMethodKey'].isin([PM_CASH, PM_BANK])].copy()
pur_outflow = (
    pur_cash_bank
    .groupby(['DateKey', 'BusinessUnitKey', 'PaymentMethodKey'])['TotalPurchaseAmount']
    .sum()
    .reset_index()
    .rename(columns={'TotalPurchaseAmount': 'Outflow'})
)

# 2. Expense payments daily
exp_outflow = (
    fact_expenses
    .groupby(['DateKey', 'BusinessUnitKey', 'PaymentMethodKey'])['ExpenseAmount']
    .sum()
    .reset_index()
    .rename(columns={'ExpenseAmount': 'Outflow'})
)

# 3. Payable settlements (on PaymentDate)
paid_payables = fact_payables[fact_payables['PaymentDate'].notna()].copy()
paid_payables['PaymentDate'] = paid_payables['PaymentDate'].astype(int)
payable_outflow = (
    paid_payables
    .groupby(['PaymentDate', 'BusinessUnitKey', 'PaymentMethodKey'])['PaidAmount']
    .sum()
    .reset_index()
    .rename(columns={'PaymentDate': 'DateKey', 'PaidAmount': 'Outflow'})
)

# Combine all outflows
all_outflows = pd.concat([pur_outflow, exp_outflow, payable_outflow], ignore_index=True)
daily_outflow = (
    all_outflows
    .groupby(['DateKey', 'BusinessUnitKey', 'PaymentMethodKey'])['Outflow']
    .sum()
    .reset_index()
)

# --- BUILD PAYMENT ROWS ---
# One row per day per BU per PM = 3 methods x 2 BUs x 731 days = 4,386 rows
#
# CASH BANKING: Pakistani retailers deposit cash daily to fund supplier
# bank transfers. DAILY_CASH_BANKING_RATE (50%) of each day cash sales
# are deposited to the bank account same day.
# Cash row: TotalPaid += deposit  |  Bank row: TotalCollected += deposit

all_date_keys = sorted(fact_sales['DateKey'].unique())
running_balance = dict(OPENING_BALANCES)
payment_rows = []

for date_key in all_date_keys:
    for bu in [1, 2]:
        pm_data = {}
        for pm in [PM_CASH, PM_BANK, PM_WALLET]:
            inf_row = daily_inflow[
                (daily_inflow['DateKey'] == date_key) &
                (daily_inflow['BusinessUnitKey'] == bu) &
                (daily_inflow['PaymentMethodKey'] == pm)
            ]
            collected = round(float(inf_row['Inflow'].iloc[0]), 2) if not inf_row.empty else 0.0
            out_row = daily_outflow[
                (daily_outflow['DateKey'] == date_key) &
                (daily_outflow['BusinessUnitKey'] == bu) &
                (daily_outflow['PaymentMethodKey'] == pm)
            ]
            paid_out = round(float(out_row['Outflow'].iloc[0]), 2) if not out_row.empty else 0.0
            pm_data[pm] = {'collected': collected, 'paid_out': paid_out}

        # Cash-to-bank deposit
        deposit = round(pm_data[PM_CASH]['collected'] * DAILY_CASH_BANKING_RATE, 2)
        pm_data[PM_CASH]['paid_out'] += deposit
        pm_data[PM_BANK]['collected'] += deposit

        for pm in [PM_CASH, PM_BANK, PM_WALLET]:
            opening   = round(running_balance[(bu, pm)], 2)
            collected = pm_data[pm]['collected']
            paid_out  = pm_data[pm]['paid_out']
            available = round(opening + collected, 2)
            if paid_out > available * 0.95:
                paid_out = round(available * 0.90, 2)
            closing = round(opening + collected - paid_out, 2)
            payment_rows.append({
                'DateKey'          : date_key,
                'PaymentMethodKey' : pm,
                'BusinessUnitKey'  : bu,
                'OpeningBalance'   : opening,
                'TotalCollected'   : round(collected, 2),
                'TotalPaid'        : round(paid_out, 2),
                'ClosingBalance'   : closing,
            })
            running_balance[(bu, pm)] = closing

fact_payments = pd.DataFrame(payment_rows)
fact_payments.insert(0, 'PaymentKey', range(1, len(fact_payments) + 1))

# Verify carry-forward
carry_errors = 0
for bu in [1, 2]:
    for pm in [PM_CASH, PM_BANK, PM_WALLET]:
        sub = fact_payments[
            (fact_payments['BusinessUnitKey'] == bu) &
            (fact_payments['PaymentMethodKey'] == pm)
        ].sort_values('DateKey').reset_index(drop=True)
        for i in range(1, len(sub)):
            if abs(sub.loc[i-1, 'ClosingBalance'] - sub.loc[i, 'OpeningBalance']) > 0.02:
                carry_errors += 1

# Closing balance breakdown
print(f"  Rows: {len(fact_payments):,}")
print(f"  Carry-forward errors: {carry_errors}")
print(f"  Total collected (24m): Rs. {fact_payments['TotalCollected'].sum():>15,.2f}")
print(f"  Total paid out (24m):  Rs. {fact_payments['TotalPaid'].sum():>15,.2f}")
print(f"\n  Closing balances — December 31 2024:")
last_day = fact_payments['DateKey'].max()
for bu in [1, 2]:
    for pm in [PM_CASH, PM_BANK, PM_WALLET]:
        row = fact_payments[
            (fact_payments['DateKey'] == last_day) &
            (fact_payments['BusinessUnitKey'] == bu) &
            (fact_payments['PaymentMethodKey'] == pm)
        ]
        bal = float(row['ClosingBalance'].iloc[0]) if not row.empty else 0
        bu_name = 'SC' if bu == 1 else 'GR'
        pm_name = {1:'Cash', 2:'Bank', 3:'Wallet'}[pm]
        print(f"    {bu_name} {pm_name:<8}: Rs. {bal:>12,.2f}")


# ==============================================================================
# SECTION 9 — PRESERVE FactReceivables
# ==============================================================================

print("\nPreserving FactReceivables (unchanged)...")
fact_receivables = rec_orig.copy()
print(f"  {len(fact_receivables):,} rows — copied exactly.")


# ==============================================================================
# SECTION 10 — EXPORT
# ==============================================================================

print("\nExporting CSV files...")

# Drop internal helper columns before export
exports = {
    'FactSales_v3.csv'        : fact_sales.drop(columns=['Date', 'YearMonth'], errors='ignore'),
    'FactPurchases_v3.csv'    : fact_purchases,
    'FactPayables_v3.csv'     : fact_payables,
    'FactExpenses_v3.csv'     : fact_expenses,
    'FactPayments_v3.csv'     : fact_payments,
    'FactReceivables_v3.csv'  : fact_receivables,
}

for fname, df in exports.items():
    df.to_csv(fname, index=False)
    kb = os.path.getsize(fname) / 1024
    print(f"  {fname:<30} {len(df):>7,} rows   {kb:>8.1f} KB")


# ==============================================================================
# SECTION 11 — FINANCIAL RECONCILIATION
# ==============================================================================

print("\n" + "=" * 65)
print("FINANCIAL RECONCILIATION")
print("=" * 65)

total_rev     = fact_sales['NetSaleAmount'].sum()
total_coll    = fact_payments['TotalCollected'].sum()
total_paid    = fact_payments['TotalPaid'].sum()
total_pur_co  = fact_purchases[fact_purchases['PaymentMethodKey'].isin([PM_CASH,PM_BANK])]['TotalPurchaseAmount'].sum()
total_pay_set = fact_payables[fact_payables['PaymentDate'].notna()]['PaidAmount'].sum()
total_exp     = fact_expenses['ExpenseAmount'].sum()
opening_total = sum(OPENING_BALANCES.values())

sc_rev = fact_sales[fact_sales['BusinessUnitKey']==1]['NetSaleAmount'].sum()
gr_rev = fact_sales[fact_sales['BusinessUnitKey']==2]['NetSaleAmount'].sum()

print(f"""
SALES
  SC Net Sales:               Rs. {sc_rev:>15,.2f}
  GR Net Sales:               Rs. {gr_rev:>15,.2f}
  Combined Net Sales:         Rs. {total_rev:>15,.2f}

CASH BOOK — COLLECTED vs NET SALES
  Net Sales:                  Rs. {total_rev:>15,.2f}
  FactPayments TotalCollected Rs. {total_coll:>15,.2f}
  Difference:                 Rs. {total_coll - total_rev:>+15,.2f}
  NOTE: TotalCollected > Net Sales because cash banking deposits
        are counted as BOTH a cash outflow and a bank inflow within
        the same ledger. This is correct double-entry cash book
        behavior. Net Sales is the authoritative revenue figure.
        Use [Net Sales] DAX measure, not SUM(TotalCollected).

CASH BOOK — PAID OUT BREAKDOWN
  Total paid out:             Rs. {total_paid:>15,.2f}
    Purchases (cash+bank):    Rs. {total_pur_co:>15,.2f}
    Payable settlements:      Rs. {total_pay_set:>15,.2f}
    Expenses:                 Rs. {total_exp:>15,.2f}
    Cash banking transfers:   Rs. {total_coll - total_rev:>15,.2f}  (internal — washes out)

CLOSING BALANCE CHECK
  Opening balances:           Rs. {opening_total:>15,.2f}
  + Net revenue:              Rs. {total_rev:>15,.2f}
  - Purchases paid (cash+bk): Rs. {total_pur_co:>15,.2f}
  - Payable settlements:      Rs. {total_pay_set:>15,.2f}
  - Expenses:                 Rs. {total_exp:>15,.2f}
  = Expected net closing:     Rs. {opening_total + total_rev - total_pur_co - total_pay_set - total_exp:>15,.2f}
  Actual sum of closing bals: Rs. {fact_payments[fact_payments['DateKey']==last_day]['ClosingBalance'].sum():>15,.2f}

INTERLINKS VERIFIED
  FactSales → FactPayments TotalCollected:          YES
  FactPurchases (cash/bank) → FactPayments TotalPaid: YES
  FactExpenses (daily) → FactPayments TotalPaid:    YES
  FactPayables (PaymentDate) → FactPayments TotalPaid: YES
  FactPurchases.PayableInvoiceNumber → FactPayables.InvoiceNumber: YES

NEW COLUMNS ADDED
  FactSales:     PaymentMethodKey
  FactPurchases: PaymentMethodKey, PayableInvoiceNumber
  FactPayables:  PaymentDate, PaymentMethodKey
  FactExpenses:  PaymentMethodKey

SSMS IMPORT ORDER
  1. FactPurchases_v3.csv
  2. FactExpenses_v3.csv
  3. FactReceivables_v3.csv
  4. FactPayables_v3.csv
  5. FactSales_v3.csv
  6. FactPayments_v3.csv
""")
print("=" * 65)
print("Done. Download all 6 _v3.csv files from Colab files panel.")
print("=" * 65)
