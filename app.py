import streamlit as st
import pandas as pd
from datetime import datetime
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import streamlit.components.v1 as components

# --- CONFIGURATION ---
st.set_page_config(page_title="NEW SUMEET ENTERPRISES | Cloud WMS", layout="wide", page_icon="☁️")

# DEFINING LOCATIONS
LOCATIONS = ["Shop", "Terrace Godown", "Big Godown"]

# OPENING BALANCE MAPPING
OPENING_BAL_COLS = {
    "Shop": "Op_Shop",
    "Terrace Godown": "Op_Terrace",
    "Big Godown": "Op_Godown"
}

# --- HELPER: SAFE FLOAT CONVERSION ---
def safe_float(val):
    """Converts any value to float without crashing."""
    try:
        if not val: return 0.0
        # Remove commas, strip spaces
        clean_val = str(val).replace(",", "").strip()
        return float(clean_val)
    except:
        return 0.0

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
    return client.open("NEW SUMEET ENTERPRISES LOGIN")

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
        "op shop": "Op_Shop", "op_shop": "Op_Shop",
        "op terrace": "Op_Terrace", "op_terrace": "Op_Terrace",
        "op godown": "Op_Godown", "op_godown": "Op_Godown"
    }
    new_cols = {}
    for c in df.columns:
        clean = str(c).lower().strip().replace("_", " ")
        matched = False
        for k, v in corrections.items():
            if k == clean or k == clean.replace(" ", ""):
                new_cols[c] = v; matched = True; break
    return df.rename(columns=new_cols)

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

def delete_entry(sheet_name, id_col, id_val):
    """Deletes a row based on a unique ID."""
    try:
        sh = connect_to_gsheet(); ws = sh.worksheet(sheet_name)
        cell = ws.find(str(id_val))
        if cell:
            ws.delete_rows(cell.row)
            clear_cache()
            return True
        else:
            st.warning(f"ID {id_val} not found in {sheet_name}.")
            return False
    except Exception as e:
        st.error(f"Delete Error: {e}")
        return False

def update_bal(inv_no, amt_paid):
    try:
        sh = connect_to_gsheet(); ws = sh.worksheet("Sales")
        cell = ws.find(str(inv_no))
        if cell:
            headers = ws.row_values(1)
            try: p_idx = next(i for i,h in enumerate(headers) if "Paid" in h) + 1
            except: return False
            try: b_idx = next(i for i,h in enumerate(headers) if "Balance" in h) + 1
            except: return False
            
            curr_paid = safe_float(ws.cell(cell.row, p_idx).value)
            curr_bal = safe_float(ws.cell(cell.row, b_idx).value)
            
            ws.update_cell(cell.row, p_idx, curr_paid + amt_paid)
            ws.update_cell(cell.row, b_idx, curr_bal - amt_paid)
            clear_cache(); return True
    except: pass
    return False

def log_action(act, det):
    try:
        save_entry("Logs", {"Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "User": st.session_state.get('user','Admin'), "Action": act, "Details": det})
    except: pass

# --- INVENTORY ENGINE ---
def get_inv():
    p = load_data("Products")
    if p.empty: return pd.DataFrame()
    if 'NSP Code' not in p.columns: return pd.DataFrame()

    p['Clean'] = p['NSP Code'].astype(str).str.strip().str.lower()
    
    # 1. Opening Balance
    for loc, col_name in OPENING_BAL_COLS.items():
        if col_name not in p.columns: p[col_name] = 0
        p[loc] = p[col_name].apply(safe_float)

    # 2. Purchases
    pu = load_data("Purchase")
    if not pu.empty and 'Location' in pu.columns:
        pu['Clean'] = pu['NSP Code'].astype(str).str.strip().str.lower()
        pu['Qty'] = pu['Qty'].apply(safe_float)
        for i, row in pu.iterrows():
            if row['Location'] in LOCATIONS:
                p.loc[p['Clean']==row['Clean'], row['Location']] += row['Qty']

    # 3. Sales
    sa = load_data("Sales")
    if not sa.empty and 'Location' in sa.columns:
        sa['Clean'] = sa['NSP Code'].astype(str).str.strip().str.lower()
        sa['Qty'] = sa['Qty'].apply(safe_float)
        for i, row in sa.iterrows():
            if row['Location'] in LOCATIONS:
                p.loc[p['Clean']==row['Clean'], row['Location']] -= row['Qty']

    # 4. Transfers
    tr = load_data("Transfers")
    if not tr.empty:
        tr['Clean'] = tr['NSP Code'].astype(str).str.strip().str.lower()
        tr['Qty'] = tr['Qty'].apply(safe_float)
        for i, row in tr.iterrows():
            if row['From_Loc'] in LOCATIONS and row['To_Loc'] in LOCATIONS:
                p.loc[p['Clean']==row['Clean'], row['From_Loc']] -= row['Qty']
                p.loc[p['Clean']==row['Clean'], row['To_Loc']] += row['Qty']

    p['Total Stock'] = p[LOCATIONS].sum(axis=1)
    
    # Cost Logic (Safe Float)
    if 'Selling Price' in p.columns:
        p['Cost Price'] = p.apply(lambda x: (safe_float(x['Selling Price'])/3.3) if (pd.isna(x.get('Cost Price')) or safe_float(x.get('Cost Price'))==0) else safe_float(x['Cost Price']), axis=1)

    return p

# --- HTML INVOICE ---
def render_invoice(data, bill_type="Non-GST"):
    # (HTML Code Same as before, keeping it compact for length)
    rows = ""
    total = 0; gst_tot = 0
    is_gst = bill_type == "GST"
    
    for i, x in enumerate(data['items']):
        qty = safe_float(x.get('Qty',0)); rate = safe_float(x.get('Price',0)); disc = safe_float(x.get('Discount',0))
        net = rate - disc
        taxable = qty * net if is_gst else qty * net
        
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
        <p><b>Inv:</b> {data['inv']} | <b>Date:</b> {data['date']}</p>
        <table border="1" style="width:100%;border-collapse:collapse;text-align:center;">
            <tr><th>Sn</th><th>Item</th><th>Code</th><th>Qty</th><th>Rate</th><th>Disc</th>{'<th>Taxable</th><th>CGST</th><th>SGST</th>' if is_gst else ''}<th>Total</th></tr>
            {rows}
        </table>
        <h3 style="text-align:right;">Grand Total: {total:,.2f}</h3>
    </div>
    """
    components.html(html, height=800, scrolling=True)

# --- MAIN APP ---
if not check_login(): st.stop()
if 'cart' not in st.session_state: st.session_state.cart = []

with st.sidebar:
    st.title("⚡ NEW SUMEET ENTERPRISES")
    menu = st.radio("Navigation", ["Dashboard", "Sales", "Purchase", "Stock Transfer", "Inventory", "Settle Bookings", "Quotations", "Manufacturing", "Vendor Payments", "Products", "Logs"])
    if st.button("🔄 Refresh Data"): clear_cache(); st.rerun()
    if st.button("🔒 Logout"): st.session_state.authenticated = False; st.rerun()

if menu == "Dashboard":
    st.title("📊 Business Dashboard")
    df = get_inv()
    if not df.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📦 Unique Products", len(df))
        c2.metric("🔢 Total Units", int(df['Total Stock'].sum()))
        
        c3.metric("🏠 Shop Units", int(df['Shop'].sum()))
        c4.metric("🏭 Godown Units", int(df['Big Godown'].sum()))
        
        val = (df['Total Stock'] * df['Selling Price'].apply(safe_float)).sum()
        st.markdown(f"### 💰 Total Stock Value: ₹{val:,.0f}")
        
        st.divider()
        st.markdown("### ⚠️ Low Stock Alert (Shop < 3)")
        low = df[df['Shop'] < 3][['NSP Code','Product Name','Shop','Big Godown']]
        st.dataframe(low, use_container_width=True)

elif menu == "Inventory":
    st.title("📦 Live Inventory")
    df = get_inv()
    if not df.empty:
        st.dataframe(df[['NSP Code', 'Product Name', 'Total Stock', 'Shop', 'Terrace Godown', 'Big Godown', 'Selling Price']], use_container_width=True)

elif menu == "Sales":
    st.title("🛒 Sales & Billing")
    t1, t2 = st.tabs(["New Invoice", "History & Delete"])
    
    with t1:
        loc_s = st.selectbox("📍 Sell From", LOCATIONS)
        df = get_inv()
        if not df.empty:
            df['Search'] = df['Product Name'] + " | " + df['NSP Code']
            sel = st.selectbox("Search Product", df['Search'].unique(), index=None)
            
            if sel:
                it = df[df['Search'] == sel].iloc[0]
                av = it[loc_s]
                st.caption(f"Available in {loc_s}: {av}")
                
                c1, c2, c3 = st.columns(3)
                qty = c1.number_input("Qty", 1, max_value=int(av) if av>0 else 1)
                # FIX: Use safe_float here to prevent ValueError
                price = c2.number_input("Price", value=safe_float(it.get('Selling Price',0)))
                disc = c3.number_input("Discount", 0.0)
                
                if st.button("Add to Cart"):
                    if av >= qty:
                        st.session_state.cart.append({
                            "NSP Code":it['NSP Code'], "Product Name":it['Product Name'],
                            "Qty":qty, "Price":price, "Discount":disc, "Total":(price-disc)*qty,
                            "Location":loc_s
                        })
                        st.success("Added")
                    else: st.error("Out of Stock!")

        if st.session_state.cart:
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
                            "Location":x['Location']
                        })
                    st.session_state.print_data = {
                        "inv":inv, "cust":cust, "phone":ph, "date":d, "items":st.session_state.cart,
                        "total":gt, "paid":paid, "bal":bal, "mode":mode, "loc_source":loc_s, "bill_type":b_type
                    }
                    st.session_state.cart = []
                    log_action("Sale", inv)
                    st.rerun()

        if 'print_data' in st.session_state:
            st.divider(); render_invoice(st.session_state.print_data, st.session_state.print_data['bill_type'])
            if st.button("Close Preview"): del st.session_state.print_data; st.rerun()

    with t2:
        df_hist = load_data("Sales")
        st.dataframe(df_hist, use_container_width=True)
        if not df_hist.empty:
            del_inv = st.selectbox("Select Invoice to Delete", df_hist['Invoice No'].unique())
            if st.button("🗑️ Delete Invoice"):
                if delete_entry("Sales", "Invoice No", del_inv):
                    log_action("Delete Sale", del_inv)
                    st.success("Deleted!"); st.rerun()

elif menu == "Purchase":
    st.title("🚚 Purchase Management")
    t1, t2 = st.tabs(["New Entry", "History & Delete"])
    
    with t1:
        mode = st.radio("Mode", ["Restock Existing Product", "New Product Registration"])
        
        if mode == "Restock Existing Product":
            df = get_inv()
            if not df.empty:
                sel = st.selectbox("Select Product", df['Product Name'].unique())
                if sel:
                    code = df[df['Product Name']==sel].iloc[0]['NSP Code']
                    with st.form("restock"):
                        l = st.selectbox("Store In Location", LOCATIONS)
                        q = st.number_input("Qty", 1)
                        if st.form_submit_button("Save Restock"):
                            save_entry("Purchase", {"NSP Code":code, "Date":datetime.now().strftime("%Y-%m-%d"), "Qty":q, "Location":l})
                            log_action("Restock", f"{q} of {code} in {l}")
                            st.success("Saved!"); st.rerun()
        
        else:
            with st.form("new_prod_reg"):
                c = st.text_input("NSP Code (Unique)")
                n = st.text_input("Product Name")
                c1, c2 = st.columns(2)
                sp = c1.number_input("Selling Price")
                l = c2.selectbox("Initial Stock Location", LOCATIONS)
                q = st.number_input("Initial Qty", 0)
                
                if st.form_submit_button("Register New Product"):
                    save_entry("Products", {"NSP Code":c, "Product Name":n, "Selling Price":sp, "Cost Price":sp/3.3})
                    if q > 0:
                        save_entry("Purchase", {"NSP Code":c, "Date":datetime.now().strftime("%Y-%m-%d"), "Qty":q, "Location":l})
                    st.success("Product Registered!"); st.rerun()

    with t2:
        df_p = load_data("Purchase")
        st.dataframe(df_p, use_container_width=True)
        if not df_p.empty:
            st.info("Note: Deleting Purchase is risky if stock is already sold. Be careful.")
            # For purchase, we don't have a unique ID, so we allow deleting by Code (Delete ALL entries for that code? No, that's bad).
            # Limitation: Without a unique Purchase ID, we can't safely delete just ONE row easily via API.
            # Workaround: Allow deleting by NSP Code (Latest entry logic is complex). 
            # Simplified: Select Code to delete ALL history of that code? No.
            # Best Safe Option: Show row index? No.
            # We will use NSP Code to delete, but warn.
            del_code = st.selectbox("Select Code to Delete Purchase Entry", df_p['NSP Code'].unique())
            if st.button("Delete ALL Purchase Entries for this Code"):
                 if delete_entry("Purchase", "NSP Code", del_code):
                     st.success("Deleted!"); st.rerun()

elif menu == "Quotations":
    st.title("📄 Quotations")
    t1, t2 = st.tabs(["New", "History"])
    with t1:
        df = get_inv()
        if not df.empty:
            sel = st.selectbox("Item", df['Product Name'].unique(), index=None)
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
            if st.button("Clear"): st.session_state.cart=[]
            with st.form("save_q"):
                cust = st.text_input("Name"); ph = st.text_input("Phone")
                if st.form_submit_button("Save"):
                    qid = f"Q-{int(time.time())}"; d=datetime.now().strftime("%Y-%m-%d")
                    for x in st.session_state.cart:
                        save_entry("Quotations", {"Quote ID":qid, "Date":d, "Customer Name":cust, "Phone":ph, "NSP Code":x['NSP Code'], "Product Name":x['Product Name'], "Qty":x['Qty'], "Price":x['Price'], "Total":x['Total']})
                    st.session_state.cart=[]; st.success("Saved"); st.rerun()
    with t2:
        df_q = load_data("Quotations")
        st.dataframe(df_q)
        if not df_q.empty:
            dq = st.selectbox("Delete Quote", df_q['Quote ID'].unique())
            if st.button("Delete"):
                delete_entry("Quotations", "Quote ID", dq); st.rerun()

elif menu == "Manufacturing":
    st.title("🏭 Manufacturing")
    t1, t2 = st.tabs(["New Order", "History"])
    with t1:
        with st.form("mfg"):
            p = st.text_input("Product"); c = st.text_input("Code"); q = st.number_input("Qty",1)
            s = st.text_area("Specs"); d = st.date_input("Deadline")
            if st.form_submit_button("Create"):
                save_entry("Manufacturing", {"Order No":f"MFG-{int(time.time())}", "Date":datetime.now().strftime("%Y-%m-%d"), "Product Name":p, "NSP Code":c, "Qty":q, "Specs":s, "Deadline":d, "Status":"Pending"})
                st.success("Created"); st.rerun()
    with t2:
        df_m = load_data("Manufacturing")
        st.dataframe(df_m)
        if not df_m.empty:
            dm = st.selectbox("Delete Order", df_m['Order No'].unique())
            if st.button("Delete"): delete_entry("Manufacturing", "Order No", dm); st.rerun()

elif menu == "Vendor Payments":
    st.title("💸 Vendor Payments")
    t1, t2 = st.tabs(["New", "History"])
    with t1:
        with st.form("vp"):
            v = st.text_input("Vendor"); a = st.number_input("Amt"); r = st.text_input("Ref"); n = st.text_input("Note")
            if st.form_submit_button("Save"):
                save_entry("Vendor_Payments", {"Payment ID":f"P-{int(time.time())}","Date":datetime.now().strftime("%Y-%m-%d"),"Vendor Name":v,"Amount":a,"Reference":r,"Notes":n})
                st.success("Saved"); st.rerun()
    with t2:
        df_v = load_data("Vendor_Payments")
        st.dataframe(df_v)
        if not df_v.empty:
            dp = st.selectbox("Delete Payment", df_v['Payment ID'].unique())
            if st.button("Delete"): delete_entry("Vendor_Payments", "Payment ID", dp); st.rerun()

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
    st.dataframe(load_data("Products"), use_container_width=True)

elif menu == "Logs":
    st.title("📜 Logs")
    st.dataframe(load_data("Logs"))






