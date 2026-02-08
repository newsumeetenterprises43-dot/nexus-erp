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

# --- GOOGLE SHEETS CONNECTION ---
@st.cache_resource
def connect_to_gsheet():
    # Looks for secrets in Streamlit Cloud, or local secrets.toml
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(st.secrets["gcp_service_account"], scope)
    client = gspread.authorize(creds)
    return client.open("nexus_erp_db")

# --- AUTHENTICATION ---
USERS = {
    "owner": "admin123",
    "manager": "user123"
}

def check_login():
    if 'authenticated' not in st.session_state: st.session_state.authenticated = False
    if not st.session_state.authenticated:
        st.markdown("<h2 style='text-align:center;'>🔒 Nexus Cloud ERP</h2>", unsafe_allow_html=True)
        c1, c2, c3 = st.columns([1,2,1])
        with c2:
            with st.form("login_form"):
                username = st.text_input("Username")
                password = st.text_input("Password", type="password")
                if st.form_submit_button("Login"):
                    if username in USERS and USERS[username] == password:
                        st.session_state.authenticated = True
                        st.session_state.user = username
                        st.success("Access Granted")
                        st.rerun()
                    else:
                        st.error("Invalid Credentials")
        return False
    return True

# --- BACKEND FUNCTIONS (GOOGLE SHEETS) ---

# --- REPLACE THE OLD load_data WITH THIS ---
@st.cache_data(ttl=10)
def load_data(sheet_name):
    try:
        sh = connect_to_gsheet()
        ws = sh.worksheet(sheet_name)
        return pd.DataFrame(ws.get_all_records())
    except Exception as e:
        # This will print the specific error on your screen
        st.error(f"❌ Error loading '{sheet_name}': {e}")
        return pd.DataFrame()

def clear_cache():
    load_data.clear()

def save_entry(sheet_name, data_dict):
    try:
        sh = connect_to_gsheet()
        try: ws = sh.worksheet(sheet_name)
        except: 
            # Create if missing
            ws = sh.add_worksheet(sheet_name, 100, 20)
            ws.append_row(list(data_dict.keys()))
            
        headers = ws.row_values(1)
        row_to_append = []
        for h in headers:
            # Flexible matching: "NSP Code" matches "nspcode", "nsp code", etc.
            val = ""
            h_clean = h.lower().replace(" ", "").strip()
            for k, v in data_dict.items():
                if k.lower().replace(" ", "").strip() == h_clean:
                    val = str(v)
                    break
            row_to_append.append(val)
            
        ws.append_row(row_to_append)
        clear_cache()
        return True
    except Exception as e:
        st.error(f"Save Error: {e}")
        return False

def delete_row(sheet_name, col_name, value):
    try:
        sh = connect_to_gsheet()
        ws = sh.worksheet(sheet_name)
        cell = ws.find(str(value)) 
        if cell:
            ws.delete_rows(cell.row)
            clear_cache()
            return True
        return False
    except: return False

def update_bal(inv_no, amt_paid):
    try:
        sh = connect_to_gsheet()
        ws = sh.worksheet("Sales")
        cell = ws.find(str(inv_no))
        if cell:
            headers = ws.row_values(1)
            p_idx = next(i for i,h in enumerate(headers) if "Paid" in h) + 1
            b_idx = next(i for i,h in enumerate(headers) if "Balance" in h) + 1
            
            curr_paid = float(ws.cell(cell.row, p_idx).value or 0)
            curr_bal = float(ws.cell(cell.row, b_idx).value or 0)
            
            ws.update_cell(cell.row, p_idx, curr_paid + amt_paid)
            ws.update_cell(cell.row, b_idx, curr_bal - amt_paid)
            clear_cache()
            return True
    except: pass
    return False

def log_action(act, det):
    try:
        save_entry("Logs", {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "User": st.session_state.get('user','Admin'),
            "Action": act, "Details": det
        })
    except: pass

def get_inv():
    """Master Inventory with Location Logic"""
    p = load_data("Products")
    if p.empty: return pd.DataFrame()
    
    p['Clean'] = p['NSP Code'].astype(str).str.strip().str.lower()
    
    # 1. Opening Balance from Columns
    for loc in LOCATIONS:
        col = f"Op_{loc.split()[0]}" # Op_Shop, Op_Terrace
        if col not in p.columns: p[col] = 0
        p[loc] = pd.to_numeric(p[col], errors='coerce').fillna(0)

    # 2. Add Purchases
    pu = load_data("Purchase")
    if not pu.empty and 'Location' in pu.columns:
        pu['Clean'] = pu['NSP Code'].astype(str).str.strip().str.lower()
        pu['Qty'] = pd.to_numeric(pu['units'], errors='coerce').fillna(0) # Using 'units' from your excel
        for i, r in pu.iterrows():
            if r['Location'] in LOCATIONS:
                p.loc[p['Clean']==r['Clean'], r['Location']] += r['Qty']

    # 3. Subtract Sales
    sa = load_data("Sales")
    if not sa.empty and 'Location' in sa.columns:
        sa['Clean'] = sa['NSP Code'].astype(str).str.strip().str.lower()
        sa['Qty'] = pd.to_numeric(sa['Qty'], errors='coerce').fillna(0)
        for i, r in sa.iterrows():
            if r['Location'] in LOCATIONS:
                p.loc[p['Clean']==r['Clean'], r['Location']] -= r['Qty']

    # 4. Handle Transfers
    tr = load_data("Transfers")
    if not tr.empty:
        tr['Clean'] = tr['NSP Code'].astype(str).str.strip().str.lower()
        tr['Qty'] = pd.to_numeric(tr['Qty'], errors='coerce').fillna(0)
        for i, r in tr.iterrows():
            if r['From_Loc'] in LOCATIONS and r['To_Loc'] in LOCATIONS:
                p.loc[p['Clean']==r['Clean'], r['From_Loc']] -= r['Qty']
                p.loc[p['Clean']==r['Clean'], r['To_Loc']] += r['Qty']

    p['Total Stock'] = p[LOCATIONS].sum(axis=1)
    
    # Cost Logic
    if 'Selling Price' in p.columns:
        if 'Cost Price' not in p.columns: p['Cost Price'] = 0.0
        p['Cost Price'] = p.apply(lambda x: (float(x['Selling Price'])/3.3) if (pd.isna(x.get('Cost Price')) or x.get('Cost Price')==0) else x['Cost Price'], axis=1)

    return p

# --- HTML INVOICE GENERATOR (PRESERVED) ---
def render_invoice(data, bill_type="Non-GST"):
    # ... [Keeping your EXACT HTML logic intact] ...
    if bill_type == "GST":
        rows = ""
        t_tax = 0; t_gst = 0
        for i, x in enumerate(data['items']):
            qty = float(x.get('Qty',0)); rate = float(x.get('Price',0)); disc = float(x.get('Discount',0))
            net = rate - disc; tax = qty * net; gst = tax * 0.18; tot = tax + gst
            t_tax += tax; t_gst += gst
            rows += f"<tr><td>{i+1}</td><td>{x['Product Name']}</td><td>{x['NSP Code']}</td><td>{qty}</td><td>{rate}</td><td>{disc}</td><td>{tax}</td><td>{gst/2}</td><td>{gst/2}</td><td>{tot}</td></tr>"
        
        g_tot = t_tax + t_gst
        html = f"""
        <div style="border:1px solid black;padding:20px;font-family:Arial;">
            <center><h2>TAX INVOICE - SUMEET ENTERPRISES</h2></center>
            <p><b>Bill To:</b> {data['cust']} | <b>Phone:</b> {data['phone']}</p>
            <p><b>Inv:</b> {data['inv']} | <b>Date:</b> {data['date']}</p>
            <table border="1" style="width:100%;border-collapse:collapse;text-align:center;">
                <tr><th>Sn</th><th>Item</th><th>HSN</th><th>Qty</th><th>Rate</th><th>Disc</th><th>Taxable</th><th>CGST</th><th>SGST</th><th>Total</th></tr>
                {rows}
            </table>
            <h3 style="text-align:right;">Total: {g_tot:,.2f}</h3>
        </div>
        """
    else:
        # Non-GST
        rows = ""
        t_amt = 0
        for i, x in enumerate(data['items']):
            qty = float(x.get('Qty',0)); rate = float(x.get('Price',0)); disc = float(x.get('Discount',0))
            tot = qty * (rate - disc)
            t_amt += tot
            rows += f"<tr><td>{i+1}</td><td>{x['Product Name']}</td><td>{x['NSP Code']}</td><td>{qty}</td><td>{rate}</td><td>{disc}</td><td>{tot}</td></tr>"
        
        html = f"""
        <div style="border:1px solid black;padding:20px;font-family:Arial;">
            <center><h2>ESTIMATE - SUMEET ENTERPRISES</h2></center>
            <p><b>M/s:</b> {data['cust']} | <b>Phone:</b> {data['phone']}</p>
            <p><b>No:</b> {data['inv']} | <b>Date:</b> {data['date']} | <b>Sold From:</b> {data.get('loc_source','Shop')}</p>
            <table border="1" style="width:100%;border-collapse:collapse;text-align:center;">
                <tr><th>Sn</th><th>Particulars</th><th>Code</th><th>Qty</th><th>Rate</th><th>Disc</th><th>Amount</th></tr>
                {rows}
            </table>
            <h3 style="text-align:right;">Grand Total: {t_amt:,.2f}</h3>
            <p>Paid: {data.get('paid',0)} | Bal: {data.get('bal',0)} | Mode: {data.get('mode','')}</p>
        </div>
        """
    components.html(html, height=800, scrolling=True)

# --- MAIN APP LOGIC ---

if not check_login(): st.stop()
if 'cart' not in st.session_state: st.session_state.cart = []

with st.sidebar:
    st.title("⚡ NEXUS ERP")
    menu = st.radio("Navigation", ["Dashboard", "Sales", "Settle Bookings", "Purchase", "Stock Transfer", "Inventory", "Quotations", "Manufacturing", "Vendor Payments", "Products", "Logs"])
    if st.button("🔄 Refresh"): clear_cache(); st.rerun()
    if st.button("🔒 Logout"): st.session_state.authenticated = False; st.rerun()

# --- PAGES ---

if menu == "Dashboard":
    st.title("📊 Business Dashboard")
    df = get_inv()
    if not df.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📦 Total Stock", int(df['Total Stock'].sum()))
        c2.metric("🏠 Shop", int(df['Shop'].sum()))
        c3.metric("🏢 Terrace", int(df['Terrace Godown'].sum()))
        c4.metric("🏭 Godown", int(df['Big Godown'].sum()))
        
        val = (df['Total Stock'] * pd.to_numeric(df['Selling Price'], errors='coerce').fillna(0)).sum()
        st.markdown(f"### 💰 Total Value: ₹{val:,.0f}")

elif menu == "Inventory":
    st.title("📦 Live Inventory")
    df = get_inv()
    if not df.empty:
        # Show location columns
        st.dataframe(df[['NSP Code','Product Name','Total Stock','Shop','Terrace Godown','Big Godown','Selling Price']], use_container_width=True)

elif menu == "Stock Transfer":
    st.title("🚚 Internal Transfer")
    df = get_inv()
    if not df.empty:
        df['S'] = df['Product Name'] + " | " + df['NSP Code']
        sel = st.selectbox("Select Product", df['S'].unique())
        if sel:
            it = df[df['S']==sel].iloc[0]
            st.code(f"Shop: {it['Shop']} | Terrace: {it['Terrace Godown']} | Godown: {it['Big Godown']}")
            with st.form("tf"):
                c1,c2,c3 = st.columns(3)
                fr = c1.selectbox("From", LOCATIONS)
                to = c2.selectbox("To", LOCATIONS)
                qty = c3.number_input("Qty",1)
                if st.form_submit_button("Transfer"):
                    if it[fr] >= qty:
                        save_entry("Transfers", {
                            "Date":datetime.now().strftime("%Y-%m-%d"),
                            "NSP Code":it['NSP Code'], "Product Name":it['Product Name'],
                            "From_Loc":fr, "To_Loc":to, "Qty":qty, "User":st.session_state.user
                        })
                        st.success("Moved!"); time.sleep(1); st.rerun()
                    else: st.error("Insufficient Stock")

elif menu == "Sales":
    st.title("🛒 Sales & Billing")
    t1, t2 = st.tabs(["New Invoice", "History"])
    
    with t1:
        loc_s = st.selectbox("📍 Selling From Location", LOCATIONS)
        df = get_inv()
        if not df.empty:
            df['Search'] = df['Product Name'] + " | " + df['NSP Code']
            sel = st.selectbox("Search", df['Search'].unique(), index=None)
            
            if sel:
                it = df[df['Search'] == sel].iloc[0]
                av = it[loc_s]
                st.info(f"Available in {loc_s}: {av}")
                
                c1, c2, c3 = st.columns(3)
                qty = c1.number_input("Qty", 1, max_value=int(av) if av>0 else 1)
                price = c2.number_input("Rate", value=float(it.get('Selling Price',0)))
                disc = c3.number_input("Discount", 0.0)
                
                if st.button("Add"):
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
            if st.button("Clear"): st.session_state.cart=[]
            
            gt = sum(x['Total'] for x in st.session_state.cart)
            st.markdown(f"### Total: {gt:,.2f}")
            
            with st.form("cout"):
                c1, c2 = st.columns(2)
                cust = c1.text_input("Name"); ph = c2.text_input("Phone")
                c3, c4 = st.columns(2)
                mode = c3.selectbox("Mode", ["Cash","UPI","Card"]); inv = c4.text_input("Inv No", value=f"INV-{int(time.time())}")
                paid = st.number_input("Paid", value=gt)
                b_type = st.radio("Type", ["Non-GST", "GST"], horizontal=True)
                
                if st.form_submit_button("Generate"):
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
            st.markdown("### Print Preview")
            render_invoice(st.session_state.print_data, st.session_state.print_data['bill_type'])
            if st.button("Close"): del st.session_state.print_data; st.rerun()

    with t2:
        st.dataframe(load_data("Sales"), use_container_width=True)

elif menu == "Purchase":
    st.title("Purchase")
    df = get_inv()
    if not df.empty:
        sel = st.selectbox("Product", df['Product Name'].unique())
        if sel:
            code = df[df['Product Name']==sel].iloc[0]['NSP Code']
            with st.form("buy"):
                l = st.selectbox("Store In", LOCATIONS)
                q = st.number_input("Qty", 1)
                if st.form_submit_button("Save"):
                    save_entry("Purchase", {"NSP Code":code, "Date":datetime.now().strftime("%Y-%m-%d"), "units":q, "Location":l})
                    st.success("Saved"); st.rerun()

elif menu == "Settle Bookings":
    st.title("Settle Bookings")
    df = load_data("Sales")
    if not df.empty and 'Balance' in df.columns:
        df['Balance'] = pd.to_numeric(df['Balance'], errors='coerce').fillna(0)
        p = df[df['Balance']>0].drop_duplicates(subset=['Invoice No'])
        if not p.empty:
            sel = st.selectbox("Pending Inv", p['Invoice No'].unique())
            r = p[p['Invoice No']==sel].iloc[0]
            st.info(f"Cust: {r['Customer Name']} | Bal: {r['Balance']}")
            amt = st.number_input("Pay Now")
            if st.button("Update"):
                update_bal(sel, amt)
                save_entry("Settlements", {"Date":datetime.now().strftime("%Y-%m-%d"),"Invoice No":sel,"Amount Paid":amt})
                st.success("Done"); st.rerun()

elif menu == "Quotations":
    st.title("Quotations")
    df = get_inv()
    if not df.empty:
        sel = st.selectbox("Item", df['Product Name'].unique(), index=None)
        if sel:
            it = df[df['Product Name']==sel].iloc[0]
            with st.form("q_add"):
                q = st.number_input("Qty",1)
                p = st.number_input("Price", value=float(it.get('Selling Price',0)))
                if st.form_submit_button("Add to Quote"):
                    st.session_state.cart.append({"NSP Code":it['NSP Code'],"Product Name":it['Product Name'],"Qty":q,"Price":p,"Total":q*p})
                    st.success("Added")
    
    if st.session_state.cart:
        st.dataframe(pd.DataFrame(st.session_state.cart))
        if st.button("Clear Q"): st.session_state.cart=[]
        with st.form("save_q"):
            cust = st.text_input("Customer"); ph = st.text_input("Phone")
            if st.form_submit_button("Save Quote"):
                qid = f"Q-{int(time.time())}"
                d = datetime.now().strftime("%Y-%m-%d")
                for x in st.session_state.cart:
                    save_entry("Quotations", {
                        "Quote ID":qid, "Date":d, "Customer Name":cust, "Phone":ph,
                        "NSP Code":x['NSP Code'], "Product Name":x['Product Name'],
                        "Qty":x['Qty'], "Price":x['Price'], "Total":x['Total']
                    })
                st.session_state.cart=[]
                st.success("Saved"); st.rerun()

elif menu == "Manufacturing":
    st.title("Manufacturing")
    with st.form("mfg"):
        p = st.text_input("Product"); c = st.text_input("Code"); q = st.number_input("Qty",1)
        s = st.text_area("Specs"); d = st.date_input("Deadline")
        if st.form_submit_button("Create Order"):
            save_entry("Manufacturing", {
                "Order No":f"MFG-{int(time.time())}", "Date":datetime.now().strftime("%Y-%m-%d"),
                "Product Name":p, "NSP Code":c, "Qty":q, "Specs":s, "Deadline":d, "Status":"Pending"
            })
            st.success("Created")
    st.dataframe(load_data("Manufacturing"))

elif menu == "Vendor Payments":
    st.title("Vendor Payments")
    with st.form("vp"):
        v = st.text_input("Vendor"); a = st.number_input("Amt"); r = st.text_input("Ref")
        if st.form_submit_button("Save"):
            save_entry("Vendor_Payments", {"Payment ID":f"P-{int(time.time())}","Date":datetime.now().strftime("%Y-%m-%d"),"Vendor Name":v,"Amount":a,"Reference":r})
            st.success("Saved")
    st.dataframe(load_data("Vendor_Payments"))

elif menu == "Products":
    st.title("Product Master")
    st.dataframe(get_inv())
    with st.form("qa"):
        c = st.text_input("Code"); n = st.text_input("Name"); sp = st.number_input("SP")
        if st.form_submit_button("Quick Add"):
            save_entry("Products", {"NSP Code":c, "Product Name":n, "Selling Price":sp})
            st.success("Added")

elif menu == "Logs":
    st.title("System Logs")

    st.dataframe(load_data("Logs"))
