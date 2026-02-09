import streamlit as st
import pandas as pd
from datetime import datetime
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import streamlit.components.v1 as components

# --- CONFIGURATION ---
st.set_page_config(page_title="NEXUS ERP | Cloud WMS", layout="wide", page_icon="☁️")

# DEFINING LOCATIONS & DATA
LOCATIONS = ["Shop", "Terrace Godown", "Big Godown"]
SALESMEN = ["Owner", "NISHIKANT", "MAYUR", "JADHAV SIR", "MASKE SIR", "LAUTE SIR", "ABDUL BHAI", "PRALHAD", "GONDIKAR SIR"]

# BANK DETAILS
BANK_DETAILS = {
    "Name": "Bank of India",
    "Account": "068230110000003",
    "IFSC": "BKID0000682",
    "Branch": "Garkheda Aurangabad"
}

# OPENING BALANCE MAPPING
OPENING_BAL_COLS = {
    "Shop": "Op_Shop",
    "Terrace Godown": "Op_Terrace",
    "Big Godown": "Op_Godown"
}

# --- HELPER: SAFE FLOAT & FORMATTING ---
def safe_float(val):
    try:
        if val is None or val == "": return 0.0
        clean_val = str(val).replace(",", "").replace("₹", "").strip()
        return float(clean_val)
    except:
        return 0.0

# --- UNIVERSAL SLICER/FILTER ---
def render_filtered_table(df, key_prefix):
    """Adds a filter bar above any table."""
    if df.empty:
        st.info("No records found.")
        return df
    
    with st.expander("🔍 Filter & Search Data", expanded=False):
        c1, c2 = st.columns([1, 2])
        all_cols = list(df.columns)
        filter_col = c1.selectbox(f"Filter Column", ["All"] + all_cols, key=f"filt_col_{key_prefix}")
        
        if filter_col != "All":
            unique_vals = df[filter_col].astype(str).unique()
            if len(unique_vals) < 30:
                val = c2.selectbox(f"Select Value", unique_vals, key=f"filt_val_{key_prefix}")
                df_filtered = df[df[filter_col].astype(str) == val]
            else:
                val = c2.text_input(f"Search Value", key=f"filt_txt_{key_prefix}")
                if val:
                    df_filtered = df[df[filter_col].astype(str).str.contains(val, case=False, na=False)]
                else:
                    df_filtered = df
        else:
            df_filtered = df
            
    st.dataframe(df_filtered, use_container_width=True)
    return df_filtered

# --- GOOGLE SHEETS CONNECTION ---
@st.cache_resource
def connect_to_gsheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    if "gcp_service_account" not in st.secrets:
        st.error("❌ Secrets not found!")
        st.stop()
    creds_dict = st.secrets["gcp_service_account"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client.open("nexus_erp_db")

USERS = {"owner": "admin123", "manager": "user123"}

def check_login():
    if 'authenticated' not in st.session_state: st.session_state.authenticated = False
    if not st.session_state.authenticated:
        st.markdown("<h2 style='text-align:center;'>🔒 Nexus Cloud ERP</h2>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns([1,2,1])
        with c2:
            with st.form("login_form"):
                u = st.text_input("Username"); p = st.text_input("Password", type="password")
                if st.form_submit_button("Login"):
                    if u in USERS and USERS[u] == p:
                        st.session_state.authenticated = True; st.session_state.user = u; st.rerun()
                    else: st.error("Invalid Credentials")
        return False
    return True

# --- BACKEND FUNCTIONS ---
def normalize_cols(df):
    if df.empty: return df
    corrections = {
        "nsp code": "NSP Code", "nspcode": "NSP Code", "code": "NSP Code",
        "product name": "Product Name", "productname": "Product Name",
        "units": "Qty", "quantity": "Qty", "qty": "Qty",
        "cost price": "Cost Price", "cp": "Cost Price",
        "selling price": "Selling Price", "sp": "Selling Price", "mrp": "Selling Price",
        "vendor name": "Vendor Name", "vendor": "Vendor Name",
        "invoice no": "Invoice No", "inv": "Invoice No",
        "location": "Location", "loc": "Location",
        "quote id": "Quote ID", "order no": "Order No", "payment id": "Payment ID",
        "salesman": "Salesman", "sales man": "Salesman",
        "status": "Status", "mode": "Mode",
        "cust gst": "Customer GST", "gstin": "Customer GST"
    }
    new_cols = {}
    for c in df.columns:
        clean = str(c).lower().strip().replace("_", " ")
        matched = False
        for k, v in corrections.items():
            if k == clean or k == clean.replace(" ", ""):
                new_cols[c] = v; matched = True; break
    
    df = df.rename(columns=new_cols)
    return df

@st.cache_data(ttl=10)
def load_data(sheet_name):
    try:
        sh = connect_to_gsheet(); ws = sh.worksheet(sheet_name)
        df = pd.DataFrame(ws.get_all_records())
        return normalize_cols(df)
    except: return pd.DataFrame()

def clear_cache(): load_data.clear()

def save_entry(sheet_name, data_dict):
    try:
        sh = connect_to_gsheet()
        try: ws = sh.worksheet(sheet_name)
        except: ws = sh.add_worksheet(sheet_name, 100, 20); ws.append_row(list(data_dict.keys()))
        
        headers = ws.row_values(1)
        if not headers:
            headers = list(data_dict.keys())
            ws.append_row(headers)

        row_to_append = []
        for h in headers:
            val = ""
            h_clean = h.lower().replace(" ", "").strip()
            for k, v in data_dict.items():
                if k.lower().replace(" ", "").strip() == h_clean:
                    val = str(v); break
            row_to_append.append(val)
        
        ws.append_row(row_to_append)
        clear_cache()
        return True
    except Exception as e: st.error(f"Save Error: {e}"); return False

def update_product_master(code, name, cp, sp):
    try:
        sh = connect_to_gsheet(); ws = sh.worksheet("Products")
        try:
            cell = ws.find(str(code))
            headers = ws.row_values(1)
            def get_col_idx(name_list):
                for i, h in enumerate(headers):
                    if h.lower().replace(" ","") in name_list: return i + 1
                return None
            idx_name = get_col_idx(["productname", "product_name"])
            idx_cp = get_col_idx(["costprice", "cp"])
            idx_sp = get_col_idx(["sellingprice", "sp", "mrp"])
            if idx_name: ws.update_cell(cell.row, idx_name, name)
            if idx_cp: ws.update_cell(cell.row, idx_cp, float(cp))
            if idx_sp: ws.update_cell(cell.row, idx_sp, float(sp))
        except gspread.exceptions.CellNotFound:
            save_entry("Products", {"NSP Code": code, "Product Name": name, "Cost Price": cp, "Selling Price": sp})
        clear_cache()
    except Exception as e: st.error(f"Master Update Error: {e}")

def update_balance(inv_no, amt_paid):
    try:
        sh = connect_to_gsheet(); ws = sh.worksheet("Sales")
        cell = ws.find(str(inv_no))
        if cell:
            headers = ws.row_values(1)
            idx_paid = headers.index("Paid") + 1
            idx_bal = headers.index("Balance") + 1
            cell_list = ws.findall(str(inv_no))
            for cell in cell_list:
                curr_paid = safe_float(ws.cell(cell.row, idx_paid).value)
                curr_bal = safe_float(ws.cell(cell.row, idx_bal).value)
                new_paid = curr_paid + amt_paid
                new_bal = curr_bal - amt_paid
                ws.update_cell(cell.row, idx_paid, new_paid)
                ws.update_cell(cell.row, idx_bal, new_bal)
            clear_cache(); return True
        else: return False
    except: return False

def delete_entry(sheet_name, id_col, id_val):
    try:
        sh = connect_to_gsheet(); ws = sh.worksheet(sheet_name)
        cell = ws.find(str(id_val))
        if cell:
            ws.delete_rows(cell.row)
            clear_cache()
            return True
        else: return False
    except: return False

def log_action(act, det):
    try:
        save_entry("Logs", {"Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "User": st.session_state.get('user','Admin'), "Action": act, "Details": det})
    except: pass

# --- INVENTORY ENGINE ---
def get_inv():
    p = load_data("Products")
    if p.empty: return pd.DataFrame()
    p['Selling Price'] = p.get('Selling Price', 0).apply(safe_float)
    p['Cost Price'] = p.get('Cost Price', 0).apply(safe_float)
    p['Clean'] = p['NSP Code'].astype(str).str.strip().str.lower()
    
    for loc in LOCATIONS: p[loc] = 0.0
    for loc, col_name in OPENING_BAL_COLS.items():
        if col_name in p.columns: p[loc] += p[col_name].apply(safe_float)

    pu = load_data("Purchase")
    if not pu.empty and 'Location' in pu.columns:
        pu['Clean'] = pu['NSP Code'].astype(str).str.strip().str.lower()
        pu['Qty'] = pu['Qty'].apply(safe_float)
        pu_grp = pu.groupby(['Clean', 'Location'])['Qty'].sum().reset_index()
        for i, row in pu_grp.iterrows():
            if row['Location'] in LOCATIONS:
                p.loc[p['Clean'] == row['Clean'], row['Location']] += row['Qty']

    sa = load_data("Sales")
    if not sa.empty and 'Location' in sa.columns:
        sa['Clean'] = sa['NSP Code'].astype(str).str.strip().str.lower()
        sa['Qty'] = sa['Qty'].apply(safe_float)
        sa_grp = sa.groupby(['Clean', 'Location'])['Qty'].sum().reset_index()
        for i, row in sa_grp.iterrows():
            if row['Location'] in LOCATIONS:
                p.loc[p['Clean'] == row['Clean'], row['Location']] -= row['Qty']

    tr = load_data("Transfers")
    if not tr.empty:
        tr['Clean'] = tr['NSP Code'].astype(str).str.strip().str.lower()
        tr['Qty'] = tr['Qty'].apply(safe_float)
        for i, row in tr.iterrows():
            if row['From_Loc'] in LOCATIONS and row['To_Loc'] in LOCATIONS:
                p.loc[p['Clean'] == row['Clean'], row['From_Loc']] -= row['Qty']
                p.loc[p['Clean'] == row['Clean'], row['To_Loc']] += row['Qty']

    p['Total Stock'] = p[LOCATIONS].sum(axis=1)
    mask_cp_0 = (p['Cost Price'] == 0) & (p['Selling Price'] > 0)
    p.loc[mask_cp_0, 'Cost Price'] = p.loc[mask_cp_0, 'Selling Price'] / 3.3
    return p

# --- HTML INVOICE ---
def render_invoice(data, bill_type="Non-GST"):
    rows = ""
    total = 0; gst_tot = 0
    is_gst = bill_type == "GST"
    items = data.get('items', [])
    
    for i, x in enumerate(items):
        qty = safe_float(x.get('Qty',0)); rate = safe_float(x.get('Price',0)); disc = safe_float(x.get('Discount',0))
        amount = qty * rate # Sold price * Qty
        
        if is_gst:
            taxable = amount
            gst_amt = taxable * 0.18
            total_line = taxable + gst_amt
            gst_tot += gst_amt
            total += total_line
            # Added HSN (9403) and NSP Code columns
            rows += f"""
            <tr>
                <td>{i+1}</td>
                <td style="text-align:left; padding-left:5px;">{x['Product Name']}</td>
                <td>{x['NSP Code']}</td>
                <td>9403</td>
                <td>{qty}</td>
                <td>{rate:,.2f}</td>
                <td>{disc:,.2f}</td>
                <td>{amount:,.2f}</td>
                <td>{gst_amt/2:,.2f}</td>
                <td>{gst_amt/2:,.2f}</td>
                <td>{total_line:,.2f}</td>
            </tr>"""
        else:
            total += amount
            # Non-GST format (HSN not strictly required but kept NSP)
            rows += f"""
            <tr>
                <td>{i+1}</td>
                <td style="text-align:left; padding-left:5px;">{x['Product Name']}</td>
                <td>{x['NSP Code']}</td>
                <td>{qty}</td>
                <td>{rate:,.2f}</td>
                <td>{disc:,.2f}</td>
                <td>{amount:,.2f}</td>
            </tr>"""

    for k in range(10 - len(items)):
        rows += f"<tr><td>&nbsp;</td><td></td><td></td>{'<td></td>' if is_gst else ''}<td></td><td></td><td></td><td></td>{'<td></td><td></td><td></td>' if is_gst else ''}</tr>"

    gst_section = ""
    gst_cols_header = '<th>Taxable</th><th>CGST</th><th>SGST</th><th>Total</th>' if is_gst else ''
    hsn_header = '<th>HSN</th>' if is_gst else ''
    
    if is_gst:
        gst_section = f"""
        <tr>
            <td colspan="8" style="text-align:right;"><b>CGST (9%):</b></td>
            <td colspan="3">{gst_tot/2:,.2f}</td>
        </tr>
        <tr>
            <td colspan="8" style="text-align:right;"><b>SGST (9%):</b></td>
            <td colspan="3">{gst_tot/2:,.2f}</td>
        </tr>
        """
    
    cust_gst_display = f"<br><b>GSTIN:</b> {data.get('cust_gst','')}" if data.get('cust_gst') else ""

    html = f"""
    <div style="width:800px; padding:20px; font-family:Arial, sans-serif; border:1px solid #000; background:white; color:black;">
        <div style="text-align:center;">
            <h1 style="margin:0; color:#b30000;">SUMEET ENTERPRISES</h1>
            <p style="margin:2px; font-size:12px;">CHETAN SUPER MARKET, TRIMURTI CHOWK, JAWAHAR COLONY ROAD, AURANGABAD-431001</p>
            <p style="margin:2px; font-size:12px;"><b>PHONE:</b> 9890834344 | <b>EMAIL:</b> sumeet.enterprises44@gmail.com</p>
            {f'<p style="margin:2px; font-size:12px;"><b>GSTIN:</b> 27AEGPC7645R1ZV</p>' if is_gst else ''}
        </div>
        <hr style="border-top: 2px solid #000;">
        <h3 style="text-align:center; margin:5px;">{'TAX INVOICE' if is_gst else 'ESTIMATE'}</h3>
        
        <table style="width:100%; border-collapse:collapse; margin-bottom:10px; font-size:12px;">
            <tr>
                <td style="border:1px solid #000; padding:5px; width:60%;">
                    <b>Customer Name:</b> {data['cust']}<br>
                    <b>Phone:</b> {data['phone']}
                    {cust_gst_display}
                </td>
                <td style="border:1px solid #000; padding:5px;">
                    <b>Invoice No:</b> {data['inv']}<br>
                    <b>Date:</b> {data['date']}<br>
                    <b>Mode:</b> {data.get('mode','')}
                </td>
            </tr>
        </table>

        <table style="width:100%; border-collapse:collapse; text-align:center; font-size:12px; border:1px solid #000;">
            <tr style="background-color:#f0f0f0;">
                <th style="border:1px solid #000; width:5%;">Sr.</th>
                <th style="border:1px solid #000; width:35%;">Product Description</th>
                <th style="border:1px solid #000;">NSP Code</th>
                {hsn_header}
                <th style="border:1px solid #000;">Qty</th>
                <th style="border:1px solid #000;">Rate</th>
                <th style="border:1px solid #000;">Disc</th>
                {gst_cols_header}
                <th style="border:1px solid #000;">Amount</th>
            </tr>
            {rows}
            {gst_section}
            <tr>
                <td colspan="{10 if is_gst else 6}" style="text-align:right; border-top:2px solid #000; padding:5px;"><b>Grand Total:</b></td>
                <td style="border-top:2px solid #000; padding:5px;"><b>{total:,.2f}</b></td>
            </tr>
        </table>
        
        <div style="margin-top:10px; border:1px solid #000; padding:5px; font-size:11px;">
            <div style="display:flex; justify-content:space-between;">
                <div style="width:60%;">
                    <b>Terms & Conditions:</b>
                    <ol style="margin:0; padding-left:20px;">
                        <li>Subject to Aurangabad jurisdiction only.</li>
                        <li>Loading/Unloading/Transport charges extra.</li>
                        <li>Once sold cannot be cancelled or returned.</li>
                    </ol>
                    <br>
                    <b>Bank Details:</b><br>
                    Bank: {BANK_DETAILS['Name']}<br>
                    Acc: {BANK_DETAILS['Account']} | IFSC: {BANK_DETAILS['IFSC']}<br>
                    Branch: {BANK_DETAILS['Branch']}
                </div>
                <div style="width:35%; text-align:center; display:flex; flex-direction:column; justify-content:end;">
                    <p><b>For SUMEET ENTERPRISES</b></p>
                    <br><br><br>
                    <p>Authorised Signatory</p>
                </div>
            </div>
        </div>
    </div>
    """
    components.html(html, height=1000, scrolling=True)

# --- MAIN APP START ---
if not check_login(): st.stop()

with st.sidebar:
    st.title("⚡ NEXUS ERP")
    menu = st.radio("Navigation", ["Dashboard", "Sales", "Settle Balance", "Purchase", "Stock Transfer", "Inventory", "Quotations", "Manufacturing", "Vendor Payments", "Products", "Logs"])
    st.divider()
    if st.button("🔄 Refresh Data"): clear_cache(); st.rerun()
    if st.button("🔒 Logout"): st.session_state.authenticated = False; st.rerun()

if 'cart' not in st.session_state: st.session_state.cart = []
if 'inv_counter' not in st.session_state: st.session_state.inv_counter = int(time.time())

# --- DASHBOARD ---
if menu == "Dashboard":
    st.title("📊 Business Dashboard")
    df = get_inv()
    if not df.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📦 Products", len(df))
        c2.metric("🔢 Total Stock", int(df['Total Stock'].sum()))
        c3.metric("🏠 Shop Stock", int(df['Shop'].sum()))
        c4.metric("🏭 Godown Stock", int(df['Big Godown'].sum()))
        
        st.divider()
        c_val1, c_val2 = st.columns(2)
        val_mrp = (df['Total Stock'] * df['Selling Price']).sum()
        val_cp = (df['Total Stock'] * df['Cost Price']).sum()
        
        c_val1.metric("💰 Asset Value (MRP)", f"₹{val_mrp:,.0f}")
        c_val2.metric("📉 Asset Value (Cost)", f"₹{val_cp:,.0f}")
        
        st.divider()
        st.markdown("### ⚠️ Low Stock Alert (Shop < 3)")
        low = df[df['Shop'] < 3][['NSP Code','Product Name','Shop','Big Godown']]
        render_filtered_table(low, "dash")

# --- INVENTORY ---
elif menu == "Inventory":
    st.title("📦 Live Inventory")
    df = get_inv()
    show_cols = ['NSP Code', 'Product Name', 'Total Stock', 'Shop', 'Terrace Godown', 'Big Godown', 'Selling Price', 'Cost Price']
    final_cols = [c for c in show_cols if c in df.columns]
    render_filtered_table(df[final_cols], "inv")

# --- SALES ---
elif menu == "Sales":
    st.title("🛒 Sales & Billing")
    t1, t2 = st.tabs(["New Invoice", "History / Reprint"])
    
    with t1:
        if 'print_data' in st.session_state:
            st.warning("⚠️ Bill Generated. Please Print or Close to continue.")
            render_invoice(st.session_state.print_data, st.session_state.print_data.get('bill_type', 'Non-GST'))
            if st.button("❌ Close Preview & Start New Bill", type="primary"): 
                del st.session_state.print_data
                st.session_state.inv_counter = int(time.time()) # Reset counter
                st.rerun()
        else:
            c_sell_1, c_sell_2 = st.columns(2)
            loc_s = c_sell_1.selectbox("📍 Sell From", LOCATIONS)
            salesman = c_sell_2.selectbox("👤 Salesman", SALESMEN)
            
            df = get_inv()
            if not df.empty:
                df['Search'] = df['Product Name'] + " | " + df['NSP Code']
                sel = st.selectbox("Search Product", df['Search'].unique(), index=None)
                
                if sel:
                    it = df[df['Search'] == sel].iloc[0]
                    av = it[loc_s]
                    mrp = safe_float(it['Selling Price'])
                    
                    st.info(f"Available: {av} | MRP: ₹{mrp}")
                    
                    c1, c2, c3 = st.columns(3)
                    qty = c1.number_input("Qty", 1, max_value=int(av) if av>0 else 1)
                    sold_at = c2.number_input("Sold At Price", value=mrp)
                    
                    calc_disc = mrp - sold_at
                    st.caption(f"Discount: ₹{calc_disc:.2f}")
                    
                    if st.button("Add to Cart"):
                        if av >= qty:
                            st.session_state.cart.append({
                                "NSP Code":it['NSP Code'], "Product Name":it['Product Name'],
                                "Qty":qty, "Price":sold_at, "Discount":calc_disc, "Total":sold_at*qty,
                                "Location":loc_s, "MRP": mrp
                            })
                            st.success("Added")
                        else: st.error("Out of Stock!")

            if st.session_state.cart:
                st.write("### 🛒 Cart")
                st.dataframe(pd.DataFrame(st.session_state.cart))
                if st.button("Clear Cart"): st.session_state.cart=[]
                
                gt = sum(x['Total'] for x in st.session_state.cart)
                st.markdown(f"### Total: ₹{gt:,.2f}")
                
                with st.form("checkout"):
                    c1, c2 = st.columns(2)
                    cust = c1.text_input("Customer Name"); ph = c2.text_input("Phone")
                    
                    # CUSTOMER GST INPUT
                    c_gst_1, c_gst_2 = st.columns(2)
                    cust_gst = c_gst_1.text_input("Customer GSTIN (Optional)")
                    mode = c_gst_2.selectbox("Mode", ["Cash","UPI942", "UPI03", "UPI681", "PHONEPE", "Card"])
                    
                    c3, c4 = st.columns(2)
                    # INVOICE MANUAL OVERRIDE LOGIC
                    default_inv = f"INV-{st.session_state.inv_counter}"
                    inv_input = c3.text_input("Inv No (Edit to Override)", value=default_inv)
                    b_type = c4.radio("Bill Type", ["Non-GST", "GST"], horizontal=True)
                    
                    paid = st.number_input("Amount Paid Now", value=gt)
                    submitted = st.form_submit_button("💾 Save Bill")
                
                if submitted:
                    d = datetime.now().strftime("%Y-%m-%d")
                    bal = gt - paid
                    
                    # Use the invoice number from the input field (overridden or default)
                    final_inv = inv_input
                    
                    for x in st.session_state.cart:
                        save_entry("Sales", {
                            "Invoice No":final_inv, "Date":d, "Customer Name":cust, "Phone":ph,
                            "NSP Code":x['NSP Code'], "Product Name":x['Product Name'],
                            "Qty":x['Qty'], "Price":x['Price'], "Discount":x['Discount'],
                            "Total":x['Total'], "Paid":paid, "Balance":bal, "Mode":mode, "Bill Type":b_type,
                            "Location":x['Location'], "Salesman": salesman, "Customer GST": cust_gst
                        })
                    
                    st.session_state.print_data = {
                        "inv":final_inv, "cust":cust, "phone":ph, "date":d, "items":st.session_state.cart,
                        "total":gt, "paid":paid, "bal":bal, "mode":mode, "loc_source":loc_s, 
                        "bill_type":b_type, "cust_gst": cust_gst
                    }
                    st.session_state.cart = []
                    log_action("Sale", final_inv)
                    st.rerun()

    with t2:
        df_hist = load_data("Sales")
        render_filtered_table(df_hist, "sales_hist")
        
        if not df_hist.empty:
            st.divider()
            sel_inv = st.selectbox("Select Invoice to Reprint/Delete", df_hist['Invoice No'].unique())
            c1, c2 = st.columns(2)
            if c1.button("Reprint Invoice"):
                inv_data = df_hist[df_hist['Invoice No'] == sel_inv]
                if not inv_data.empty:
                    first = inv_data.iloc[0]
                    items = [{"Product Name": r['Product Name'], "NSP Code": r['NSP Code'],"Qty": r['Qty'], "Price": r['Price'], "Discount": r.get('Discount', 0)} for i, r in inv_data.iterrows()]
                    st.session_state.print_data = {
                        "inv": sel_inv, "cust": first['Customer Name'], "phone": first['Phone'],
                        "date": first['Date'], "items": items, "mode": first.get('Mode',''),
                        "bill_type": first.get('Bill Type', 'Non-GST'), "cust_gst": first.get('Customer GST', '')
                    }
                    st.rerun()
            
            if c2.button("❌ Delete Invoice"):
                if delete_entry("Sales", "Invoice No", sel_inv):
                    log_action("Delete Sale", sel_inv)
                    st.success("Deleted!"); st.rerun()

# --- SETTLE BALANCE ---
elif menu == "Settle Balance":
    st.title("💰 Settle Pending Balance")
    df_s = load_data("Sales")
    if not df_s.empty:
        df_s['Balance'] = df_s['Balance'].apply(safe_float)
        pending = df_s[df_s['Balance'] > 0].drop_duplicates(subset=['Invoice No'])
        if pending.empty:
            st.success("🎉 No Pending Payments!")
        else:
            st.markdown("### 📋 Pending Invoices")
            st.dataframe(pending[['Invoice No', 'Date', 'Customer Name', 'Phone', 'Total', 'Paid', 'Balance']], use_container_width=True)
            st.divider()
            sel_inv_pay = st.selectbox("Select Invoice to Settle", pending['Invoice No'].unique())
            if sel_inv_pay:
                row = pending[pending['Invoice No'] == sel_inv_pay].iloc[0]
                curr_bal = row['Balance']
                cust_name = row['Customer Name']
                st.info(f"Customer: {cust_name} | Current Balance: ₹{curr_bal}")
                with st.form("settle_form"):
                    pay_amt = st.number_input("Enter Amount to Pay", 1.0, max_value=float(curr_bal))
                    pay_mode = st.selectbox("Payment Mode", ["Cash", "UPI", "Card"])
                    note = st.text_input("Note (Optional)")
                    if st.form_submit_button("Confirm Payment"):
                        if update_balance(sel_inv_pay, pay_amt):
                            st.success("Payment Recorded!")
                            log_action("Settlement", f"{sel_inv_pay} - {pay_amt}")
                            html = f"""
                            <div style="border:1px solid black; padding:20px; font-family:Arial;">
                                <center><h2>PAYMENT RECEIPT</h2></center>
                                <p><b>Date:</b> {datetime.now().strftime("%Y-%m-%d")}</p>
                                <p><b>Received with thanks from:</b> {cust_name}</p>
                                <p><b>Against Invoice No:</b> {sel_inv_pay}</p>
                                <p><b>Sum of Rupees:</b> ₹{pay_amt:,.2f}</p>
                                <p><b>Mode:</b> {pay_mode}</p>
                                <p><b>Remaining Balance:</b> ₹{curr_bal - pay_amt:,.2f}</p>
                                <br>
                                <p style="text-align:right;">For SUMEET ENTERPRISES</p>
                            </div>
                            """
                            components.html(html, height=400)
                        else: st.error("Error updating database.")

# --- PURCHASE ---
elif menu == "Purchase":
    st.title("🚚 Purchase & Stock In")
    t1, t2 = st.tabs(["New Entry", "History"])
    
    with t1:
        mode = st.radio("Select Action", ["Restock Existing Product", "Register New Product"], horizontal=True)
        st.divider()
        if 'p_cp' not in st.session_state: st.session_state.p_cp = 0.0
        if 'p_sp' not in st.session_state: st.session_state.p_sp = 0.0

        def update_sp(): st.session_state.p_sp = st.session_state.p_cp * 1.1 * 3
        def update_cp(): st.session_state.p_cp = st.session_state.p_sp / 3.3

        if mode == "Restock Existing Product":
            df = get_inv()
            if not df.empty:
                df['Display'] = df['Product Name'] + " | " + df['NSP Code']
                sel_display = st.selectbox("Select Product", df['Display'].unique())
                if sel_display:
                    sel_prod = df[df['Display'] == sel_display].iloc[0]
                    c1, c2 = st.columns(2)
                    p_code = c1.text_input("NSP Code", value=sel_prod['NSP Code'], disabled=True)
                    p_name = c2.text_input("Product Name", value=sel_prod['Product Name'], disabled=True)
                    c3, c4 = st.columns(2)
                    db_cp = safe_float(sel_prod.get('Cost Price', 0))
                    db_sp = safe_float(sel_prod.get('Selling Price', 0))
                    input_cp = c3.number_input("Cost Price", value=db_cp)
                    input_sp = c4.number_input("Selling Price (MRP)", value=db_sp)
                    c5, c6 = st.columns(2)
                    loc = c5.selectbox("Location", LOCATIONS)
                    qty = c6.number_input("Qty", 1)
                    vendor_name = st.text_input("Vendor Name (Compulsory)")
                    if st.button("Save Restock", type="primary"):
                        if not vendor_name: st.error("⚠️ Vendor Name is Compulsory!")
                        else:
                            d = datetime.now().strftime("%Y-%m-%d")
                            update_product_master(p_code, p_name, input_cp, input_sp)
                            save_entry("Purchase", {"NSP Code": p_code, "Date": d, "Qty": qty, "Location": loc, "Vendor Name": vendor_name, "Cost Price": input_cp})
                            save_entry("Vendor_Payments", {"Payment ID": f"PEND-{int(time.time())}", "Date": d, "Vendor Name": vendor_name, "Amount": input_cp * qty, "Status": "Pending", "Notes": f"Restock {p_code}"})
                            st.success("Restocked & Payment Logged!"); st.rerun()

        else: 
            c1, c2 = st.columns(2)
            code = c1.text_input("New NSP Code")
            name = c2.text_input("New Product Name")
            c3, c4 = st.columns(2)
            cp_in = c3.number_input("Cost Price", key='p_cp', on_change=update_sp, step=1.0)
            sp_in = c4.number_input("Selling Price (MRP)", key='p_sp', on_change=update_cp, step=1.0)
            c_l1, c_l2 = st.columns(2)
            loc = c_l1.selectbox("Location", LOCATIONS)
            qty = c_l2.number_input("Qty", 1)
            vendor_name = st.text_input("Vendor Name (Compulsory)")
            if st.button("Register & Save Purchase", type="primary"):
                if not vendor_name or not code or not name: st.error("⚠️ Vendor Name, Code and Product Name are Compulsory!")
                else:
                    d = datetime.now().strftime("%Y-%m-%d")
                    update_product_master(code, name, st.session_state.p_cp, st.session_state.p_sp)
                    save_entry("Purchase", {"NSP Code": code, "Date": d, "Qty": qty, "Location": loc, "Vendor Name": vendor_name, "Cost Price": st.session_state.p_cp})
                    save_entry("Vendor_Payments", {"Payment ID": f"PEND-{int(time.time())}", "Date": d, "Vendor Name": vendor_name, "Amount": st.session_state.p_cp * qty, "Status": "Pending", "Notes": f"New: {code}"})
                    st.success("New Product Registered & Stocked!"); st.rerun()

    with t2:
        df_p = load_data("Purchase")
        # ENHANCED PURCHASE HISTORY: Merge with Products to get Names
        df_prods = load_data("Products")
        if not df_p.empty and not df_prods.empty:
            # Merge to get Product Name and Selling Price/Cost Price into Purchase History
            df_merged = pd.merge(df_p, df_prods[['NSP Code', 'Product Name', 'Selling Price', 'Cost Price']], on='NSP Code', how='left')
            render_filtered_table(df_merged, "purch")
        else:
            render_filtered_table(df_p, "purch")

# --- QUOTATIONS ---
elif menu == "Quotations":
    st.title("📄 Quotations")
    t1, t2 = st.tabs(["New Quote", "History / Reprint"])
    with t1:
        df = get_inv()
        if not df.empty:
            sel = st.selectbox("Item", df['Product Name'].unique(), index=None, key="q_sel")
            if sel:
                it = df[df['Product Name']==sel].iloc[0]
                with st.form("q_add"):
                    q = st.number_input("Qty",1)
                    p = st.number_input("Price", value=safe_float(it.get('Selling Price',0)))
                    if st.form_submit_button("Add"):
                        st.session_state.cart.append({"NSP Code":it['NSP Code'],"Product Name":it['Product Name'],"Qty":q,"Price":p,"Total":q*p})
                        st.success("Added")
        if st.session_state.cart:
            st.dataframe(pd.DataFrame(st.session_state.cart))
            if st.button("Clear Quote"): st.session_state.cart=[]
            with st.form("save_q"):
                cust = st.text_input("Customer Name"); ph = st.text_input("Phone")
                if st.form_submit_button("Save & Print"):
                    qid = f"Q-{int(time.time())}"; d=datetime.now().strftime("%Y-%m-%d")
                    for x in st.session_state.cart:
                        save_entry("Quotations", {"Quote ID":qid, "Date":d, "Customer Name":cust, "Phone":ph, "NSP Code":x['NSP Code'], "Product Name":x['Product Name'], "Qty":x['Qty'], "Price":x['Price'], "Total":x['Total']})
                    st.session_state.print_data = {"inv":qid, "cust":cust, "phone":ph, "date":d, "items":st.session_state.cart, "bill_type":"Non-GST"} 
                    st.session_state.cart=[]; st.rerun()
    with t2:
        df_q = load_data("Quotations")
        render_filtered_table(df_q, "quote_hist")
        if not df_q.empty:
            sel_q = st.selectbox("Reprint Quote ID", df_q['Quote ID'].unique())
            if st.button("Reprint Quote"):
                 q_data = df_q[df_q['Quote ID'] == sel_q]
                 if not q_data.empty:
                    first = q_data.iloc[0]
                    items = [{"Product Name":r['Product Name'],"NSP Code":r['NSP Code'],"Qty":r['Qty'],"Price":r['Price'],"Discount":0} for i,r in q_data.iterrows()]
                    st.session_state.print_data = {"inv": sel_q, "cust": first['Customer Name'], "phone": first['Phone'], "date": first['Date'], "items": items, "bill_type": "Non-GST"}
                    st.rerun()

# --- MANUFACTURING ---
elif menu == "Manufacturing":
    st.title("🏭 Manufacturing")
    t1, t2 = st.tabs(["New Order", "History"])
    with t1:
        with st.form("mfg"):
            p = st.text_input("Product Name"); c = st.text_input("NSP Code (Will Auto-Create)"); q = st.number_input("Qty",1)
            s = st.text_area("Specs"); d = st.date_input("Deadline")
            if st.form_submit_button("Create"):
                save_entry("Manufacturing", {"Order No":f"MFG-{int(time.time())}", "Date":datetime.now().strftime("%Y-%m-%d"), "Product Name":p, "NSP Code":c, "Qty":q, "Specs":s, "Deadline":d, "Status":"Pending"})
                st.success("Created"); st.rerun()
    with t2:
        df_m = load_data("Manufacturing")
        render_filtered_table(df_m, "mfg")

# --- VENDOR PAYMENTS ---
elif menu == "Vendor Payments":
    st.title("💸 Vendor Payments")
    t1, t2 = st.tabs(["New Payment", "History"])
    with t1:
        with st.form("vp"):
            v = st.text_input("Vendor"); a = st.number_input("Amt"); r = st.text_input("Ref"); n = st.text_input("Note")
            if st.form_submit_button("Save"):
                save_entry("Vendor_Payments", {"Payment ID":f"P-{int(time.time())}","Date":datetime.now().strftime("%Y-%m-%d"),"Vendor Name":v,"Amount":a,"Reference":r,"Notes":n})
                st.success("Saved"); st.rerun()
    with t2:
        df_v = load_data("Vendor_Payments")
        render_filtered_table(df_v, "vp")

# --- STOCK TRANSFER ---
elif menu == "Stock Transfer":
    st.title("🚚 Transfer")
    df = get_inv()
    if not df.empty:
        df['S'] = df['Product Name'] + " | " + df['NSP Code']
        sel = st.selectbox("Select Product", df['S'].unique())
        if sel:
            it = df[df['S']==sel].iloc[0]
            st.info(f"Shop: {it['Shop']} | Terrace: {it['Terrace Godown']} | Godown: {it['Big Godown']}")
            with st.form("tf"):
                f = st.selectbox("From", LOCATIONS); t = st.selectbox("To", LOCATIONS); q = st.number_input("Qty",1)
                if st.form_submit_button("Move"):
                    if it[f] >= q:
                        save_entry("Transfers", {"Date":datetime.now().strftime("%Y-%m-%d"),"NSP Code":it['NSP Code'],"From_Loc":f,"To_Loc":t,"Qty":q})
                        st.success("Moved"); st.rerun()
                    else: st.error("Low Stock")

# --- PRODUCTS MANAGEMENT ---
elif menu == "Products":
    st.title("📦 Products List")
    t1, t2, t3 = st.tabs(["View / Filter", "Add New Product", "Delete Product"])
    
    with t1:
        df = load_data("Products")
        render_filtered_table(df, "prods")
        
    with t2:
        st.write("### Add Product Manually (Without Purchase)")
        with st.form("add_prod_man"):
            nc = st.text_input("NSP Code")
            nn = st.text_input("Product Name")
            ncp = st.number_input("Cost Price")
            nsp = st.number_input("Selling Price")
            if st.form_submit_button("Add Product"):
                update_product_master(nc, nn, ncp, nsp)
                st.success("Product Added")
                
    with t3:
        st.write("### ⚠️ Delete Product")
        st.warning("Deleting a product does not delete its history in Sales/Purchase.")
        df = load_data("Products")
        if not df.empty:
            del_code = st.selectbox("Select Code to Delete", df['NSP Code'].unique())
            if st.button("Permanently Delete"):
                if delete_entry("Products", "NSP Code", del_code):
                    st.success(f"Deleted {del_code}")
                    st.rerun()

# --- LOGS ---
elif menu == "Logs":
    st.title("📜 Logs")
    df = load_data("Logs")
    render_filtered_table(df, "logs")








