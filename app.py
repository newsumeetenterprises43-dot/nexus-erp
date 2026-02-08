import streamlit as st
import pandas as pd
from datetime import datetime
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import streamlit.components.v1 as components

# --- CONFIGURATION ---
st.set_page_config(page_title="NEXUS ERP | Cloud WMS", layout="wide", page_icon="☁️")

# DEFINING LOCATIONS
LOCATIONS = ["Shop", "Terrace Godown", "Big Godown"]
SALESMEN = ["Owner", "Manager", "Salesman 1", "Salesman 2"]

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
        clean_val = str(val).replace(",", "").strip()
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
        filter_col = c1.selectbox(f"Filter Column", ["All"] + list(df.columns), key=f"filt_col_{key_prefix}")
        
        if filter_col != "All":
            unique_vals = df[filter_col].astype(str).unique()
            if len(unique_vals) < 20:
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
        "salesman": "Salesman", "sales man": "Salesman"
    }
    new_cols = {}
    for c in df.columns:
        clean = str(c).lower().strip().replace("_", " ")
        matched = False
        for k, v in corrections.items():
            if k == clean or k == clean.replace(" ", ""):
                new_cols[c] = v; matched = True; break
    
    df = df.rename(columns=new_cols)
    df = df.fillna(0) 
    return df

@st.cache_data(ttl=10)
def load_data(sheet_name):
    try:
        sh = connect_to_gsheet(); ws = sh.worksheet(sheet_name)
        return normalize_cols(pd.DataFrame(ws.get_all_records()))
    except: return pd.DataFrame()

def clear_cache(): load_data.clear()

def save_entry(sheet_name, data_dict):
    try:
        sh = connect_to_gsheet()
        try: ws = sh.worksheet(sheet_name)
        except: ws = sh.add_worksheet(sheet_name, 100, 20); ws.append_row(list(data_dict.keys()))
        
        headers = ws.row_values(1)
        
        # --- AUTO CREATE PRODUCT LOGIC ---
        if sheet_name in ["Purchase", "Manufacturing"] and "NSP Code" in data_dict:
             ensure_product_exists(data_dict["NSP Code"], data_dict.get("Product Name", "Auto-Created"), data_dict.get("Selling Price", 0), data_dict.get("Cost Price", 0))

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

def ensure_product_exists(code, name, sp, cp):
    """Checks if product exists, if not, adds it to Products sheet."""
    try:
        df_p = load_data("Products")
        # Check if code exists
        if df_p.empty or str(code) not in df_p['NSP Code'].astype(str).values:
            save_entry("Products", {
                "NSP Code": code, 
                "Product Name": name, 
                "Selling Price": sp, 
                "Cost Price": cp,
                "Op_Shop":0, "Op_Terrace":0, "Op_Godown":0 
            })
    except: pass

def delete_entry(sheet_name, id_col, id_val):
    try:
        sh = connect_to_gsheet(); ws = sh.worksheet(sheet_name)
        cell = ws.find(str(id_val))
        if cell:
            ws.delete_rows(cell.row)
            clear_cache()
            return True
        else: return False
    except Exception as e: st.error(f"Delete Error: {e}"); return False

def log_action(act, det):
    try:
        save_entry("Logs", {"Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "User": st.session_state.get('user','Admin'), "Action": act, "Details": det})
    except: pass

# --- INVENTORY ENGINE ---
def get_inv():
    p = load_data("Products")
    if p.empty: return pd.DataFrame()
    
    # Force Numeric Types for Critical Columns
    for col in ['Selling Price', 'Cost Price']:
        if col in p.columns:
            p[col] = p[col].apply(safe_float)
    
    for loc in LOCATIONS:
        if loc not in p.columns: p[loc] = 0.0
        p[loc] = p[loc].apply(safe_float)
        
    for loc, col_name in OPENING_BAL_COLS.items():
        if col_name in p.columns:
            p[loc] += p[col_name].apply(safe_float)

    p['Clean'] = p['NSP Code'].astype(str).str.strip().str.lower()
    
    pu = load_data("Purchase")
    if not pu.empty and 'Location' in pu.columns:
        pu['Clean'] = pu['NSP Code'].astype(str).str.strip().str.lower()
        pu['Qty'] = pu['Qty'].apply(safe_float)
        pu_grp = pu.groupby(['Clean', 'Location'])['Qty'].sum().reset_index()
        for i, row in pu_grp.iterrows():
            if row['Location'] in LOCATIONS:
                p.loc[p['Clean']==row['Clean'], row['Location']] += row['Qty']

    sa = load_data("Sales")
    if not sa.empty and 'Location' in sa.columns:
        sa['Clean'] = sa['NSP Code'].astype(str).str.strip().str.lower()
        sa['Qty'] = sa['Qty'].apply(safe_float)
        sa_grp = sa.groupby(['Clean', 'Location'])['Qty'].sum().reset_index()
        for i, row in sa_grp.iterrows():
            if row['Location'] in LOCATIONS:
                p.loc[p['Clean']==row['Clean'], row['Location']] -= row['Qty']

    tr = load_data("Transfers")
    if not tr.empty:
        tr['Clean'] = tr['NSP Code'].astype(str).str.strip().str.lower()
        tr['Qty'] = tr['Qty'].apply(safe_float)
        for i, row in tr.iterrows():
            if row['From_Loc'] in LOCATIONS and row['To_Loc'] in LOCATIONS:
                p.loc[p['Clean']==row['Clean'], row['From_Loc']] -= row['Qty']
                p.loc[p['Clean']==row['Clean'], row['To_Loc']] += row['Qty']

    p['Total Stock'] = p[LOCATIONS].sum(axis=1)
    
    # --- PRICING FORMULA SYNC ---
    if 'Selling Price' in p.columns and 'Cost Price' in p.columns:
        mask_cp_0 = (p['Cost Price'].apply(safe_float) == 0) & (p['Selling Price'].apply(safe_float) > 0)
        p.loc[mask_cp_0, 'Cost Price'] = p.loc[mask_cp_0, 'Selling Price'].apply(safe_float) / 3.3
        
    return p

# --- HTML INVOICE ---
def render_invoice(data, bill_type="Non-GST"):
    rows = ""
    total = 0; gst_tot = 0
    is_gst = bill_type == "GST"
    items = data.get('items', [])
    
    for i, x in enumerate(items):
        qty = safe_float(x.get('Qty',0)); rate = safe_float(x.get('Price',0)); disc = safe_float(x.get('Discount',0))
        net_item = rate 
        taxable = qty * net_item
        
        if is_gst:
            gst = taxable * 0.18; line_tot = taxable + gst
            gst_tot += gst; total += line_tot
            rows += f"<tr><td>{i+1}</td><td>{x['Product Name']}</td><td>{x['NSP Code']}</td><td>{qty}</td><td>{rate}</td><td>{disc}</td><td>{taxable:.2f}</td><td>{gst/2:.2f}</td><td>{gst/2:.2f}</td><td>{line_tot:.2f}</td></tr>"
        else:
            line_tot = taxable
            total += line_tot
            rows += f"<tr><td>{i+1}</td><td>{x['Product Name']}</td><td>{x['NSP Code']}</td><td>{qty}</td><td>{rate}</td><td>{disc}</td><td>{line_tot:.2f}</td></tr>"

    html = f"""
    <div style="border:1px solid black;padding:20px;font-family:Arial;background:white;color:black;">
        <center><h2>{'TAX INVOICE' if is_gst else 'ESTIMATE'} - SUMEET ENTERPRISES</h2></center>
        <p><b>Bill To:</b> {data['cust']} | <b>Phone:</b> {data['phone']}</p>
        <p><b>Inv:</b> {data['inv']} | <b>Date:</b> {data['date']} | <b>Mode:</b> {data.get('mode','Cash')}</p>
        <table border="1" style="width:100%;border-collapse:collapse;text-align:center;">
            <tr><th>Sn</th><th>Item</th><th>Code</th><th>Qty</th><th>Sold Rate</th><th>Disc/Unit</th>{'<th>Taxable</th><th>CGST</th><th>SGST</th>' if is_gst else ''}<th>Total</th></tr>
            {rows}
        </table>
        <h3 style="text-align:right;">Grand Total: {total:,.2f}</h3>
    </div>
    """
    components.html(html, height=800, scrolling=True)

# --- MAIN APP ---
if not check_login(): st.stop()

# Print Preview Handler (Top of App)
if 'print_data' in st.session_state:
    st.markdown("### 🖨️ Print Preview")
    render_invoice(st.session_state.print_data, st.session_state.print_data.get('bill_type', 'Non-GST'))
    if st.button("❌ Close Preview", type="primary"): 
        del st.session_state.print_data
        st.rerun()
    st.divider()

if 'cart' not in st.session_state: st.session_state.cart = []

with st.sidebar:
    st.title("⚡ NEXUS ERP")
    menu = st.radio("Navigation", ["Dashboard", "Sales", "Purchase", "Stock Transfer", "Inventory", "Quotations", "Manufacturing", "Vendor Payments", "Products", "Logs"])
    if st.button("🔄 Refresh Data"): clear_cache(); st.rerun()

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
        val = (df['Total Stock'] * df['Selling Price'].apply(safe_float)).sum()
        st.markdown(f"### 💰 Total Asset Value: ₹{val:,.0f}")
        
        st.divider()
        st.markdown("### ⚠️ Low Stock Alert")
        low = df[df['Shop'] < 3][['NSP Code','Product Name','Shop','Big Godown']]
        render_filtered_table(low, "dash")

elif menu == "Inventory":
    st.title("📦 Live Inventory")
    df = get_inv()
    render_filtered_table(df[['NSP Code', 'Product Name', 'Total Stock', 'Shop', 'Terrace Godown', 'Big Godown', 'Selling Price', 'Cost Price']], "inv")

elif menu == "Sales":
    st.title("🛒 Sales & Billing")
    t1, t2 = st.tabs(["New Invoice", "History / Reprint"])
    
    with t1:
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
                # FIX: Explicitly convert MRP to float to avoid 0 issue
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
                c3, c4 = st.columns(2)
                mode = c3.selectbox("Mode", ["Cash","UPI","Card"]); inv = c4.text_input("Inv No", value=f"INV-{int(time.time())}")
                paid = st.number_input("Amount Paid", value=gt)
                b_type = st.radio("Bill Type", ["Non-GST", "GST"], horizontal=True)
                
                if st.form_submit_button("Generate Invoice"):
                    d = datetime.now().strftime("%Y-%m-%d")
                    bal = gt - paid
                    for x in st.session_state.cart:
                        save_entry("Sales", {
                            "Invoice No":inv, "Date":d, "Customer Name":cust, "Phone":ph,
                            "NSP Code":x['NSP Code'], "Product Name":x['Product Name'],
                            "Qty":x['Qty'], "Price":x['Price'], "Discount":x['Discount'],
                            "Total":x['Total'], "Paid":paid, "Balance":bal, "Mode":mode, "Bill Type":b_type,
                            "Location":x['Location'], "Salesman": salesman
                        })
                    st.session_state.print_data = {
                        "inv":inv, "cust":cust, "phone":ph, "date":d, "items":st.session_state.cart,
                        "total":gt, "paid":paid, "bal":bal, "mode":mode, "loc_source":loc_s, "bill_type":b_type
                    }
                    st.session_state.cart = []
                    log_action("Sale", inv)
                    st.rerun()

    with t2:
        df_hist = load_data("Sales")
        df_filtered = render_filtered_table(df_hist, "sales_hist")
        
        if not df_filtered.empty:
            st.divider()
            sel_inv = st.selectbox("Select Invoice", df_filtered['Invoice No'].unique())
            c1, c2 = st.columns(2)
            if c1.button("Reprint Invoice"):
                inv_data = df_hist[df_hist['Invoice No'] == sel_inv]
                if not inv_data.empty:
                    first = inv_data.iloc[0]
                    items = [{"Product Name": r['Product Name'], "NSP Code": r['NSP Code'],"Qty": r['Qty'], "Price": r['Price'], "Discount": r.get('Discount', 0)} for i, r in inv_data.iterrows()]
                    st.session_state.print_data = {
                        "inv": sel_inv, "cust": first['Customer Name'], "phone": first['Phone'],
                        "date": first['Date'], "items": items, "mode": first.get('Mode',''),
                        "bill_type": first.get('Bill Type', 'Non-GST')
                    }
                    st.rerun()
            
            if c2.button("❌ Delete Invoice"):
                if delete_entry("Sales", "Invoice No", sel_inv):
                    log_action("Delete Sale", sel_inv)
                    st.success("Deleted!"); st.rerun()

elif menu == "Purchase":
    st.title("🚚 Purchase & Stock In")
    t1, t2 = st.tabs(["New Entry", "History"])
    
    with t1:
        # --- NEW MODE TOGGLE ---
        mode = st.radio("Select Action", ["Restock Existing Product", "Register New Product"], horizontal=True)
        
        # Session State for Pricing Logic
        if 'p_cp' not in st.session_state: st.session_state.p_cp = 0.0
        if 'p_sp' not in st.session_state: st.session_state.p_sp = 0.0

        def update_sp():
            st.session_state.p_sp = st.session_state.p_cp * 1.1 * 3
            
        def update_cp():
            st.session_state.p_cp = st.session_state.p_sp / 3.3

        st.divider()

        if mode == "Restock Existing Product":
            df = get_inv()
            if not df.empty:
                sel_prod = st.selectbox("Select Product to Restock", df['Product Name'].unique())
                if sel_prod:
                    # Auto-Fill details from existing database
                    curr_item = df[df['Product Name'] == sel_prod].iloc[0]
                    
                    c1, c2 = st.columns(2)
                    p_code = c1.text_input("NSP Code", value=curr_item['NSP Code'], disabled=True)
                    p_name = c2.text_input("Product Name", value=curr_item['Product Name'], disabled=True)
                    
                    c3, c4 = st.columns(2)
                    # Pre-fill Cost & SP from DB
                    db_cp = safe_float(curr_item.get('Cost Price', 0))
                    db_sp = safe_float(curr_item.get('Selling Price', 0))
                    
                    # Allow user to update price if it changed
                    input_cp = c3.number_input("Cost Price (Update if needed)", value=db_cp)
                    input_sp = c4.number_input("Selling Price (Update if needed)", value=db_sp)
                    
                    c5, c6 = st.columns(2)
                    loc = c5.selectbox("Location", LOCATIONS)
                    qty = c6.number_input("Qty", 1)
                    
                    vendor_name = st.text_input("Vendor Name (Compulsory)")
                    
                    if st.button("Save Restock", type="primary"):
                        if not vendor_name:
                            st.error("⚠️ Vendor Name is Compulsory!")
                        else:
                            d = datetime.now().strftime("%Y-%m-%d")
                            # Update Price in Master
                            save_entry("Products", {"NSP Code": p_code, "Cost Price": input_cp, "Selling Price": input_sp})
                            # Save Purchase
                            save_entry("Purchase", {"NSP Code": p_code, "Date": d, "Qty": qty, "Location": loc, "Vendor Name": vendor_name, "Cost Price": input_cp})
                            # Auto-Log Vendor Payment (Pending)
                            save_entry("Vendor_Payments", {"Payment ID": f"PEND-{int(time.time())}", "Date": d, "Vendor Name": vendor_name, "Amount": input_cp * qty, "Status": "Pending", "Notes": f"Restock {p_code}"})
                            st.success("Restocked & Payment Pending Logged!"); st.rerun()

        else: # Register New Product
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
                if not vendor_name or not code or not name:
                    st.error("⚠️ Vendor Name, Code and Product Name are Compulsory!")
                else:
                    d = datetime.now().strftime("%Y-%m-%d")
                    
                    # 1. Create Product
                    save_entry("Products", {"NSP Code": code, "Product Name": name, "Cost Price": st.session_state.p_cp, "Selling Price": st.session_state.p_sp})
                    
                    # 2. Add Purchase
                    save_entry("Purchase", {"NSP Code": code, "Date": d, "Qty": qty, "Location": loc, "Vendor Name": vendor_name, "Cost Price": st.session_state.p_cp})
                    
                    # 3. Vendor Payment
                    save_entry("Vendor_Payments", {"Payment ID": f"PEND-{int(time.time())}", "Date": d, "Vendor Name": vendor_name, "Amount": st.session_state.p_cp * qty, "Status": "Pending", "Notes": f"New: {code}"})
                    
                    st.success("New Product Registered & Stocked!"); st.rerun()

    with t2:
        df_p = load_data("Purchase")
        render_filtered_table(df_p, "purch")

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

elif menu == "Products":
    st.title("📦 Products List")
    df = load_data("Products")
    render_filtered_table(df, "prods")

elif menu == "Logs":
    st.title("📜 Logs")
    df = load_data("Logs")
    render_filtered_table(df, "logs")

