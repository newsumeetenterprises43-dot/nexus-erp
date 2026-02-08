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

# --- GOOGLE SHEETS CONNECTION ---
@st.cache_resource
def connect_to_gsheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Check if secrets exist in Streamlit Cloud
    if "gcp_service_account" not in st.secrets:
        st.error("❌ Secrets not found! Please check App Settings > Secrets.")
        st.stop()
    
    # Load credentials directly from the TOML dictionary
    creds_dict = st.secrets["gcp_service_account"]
    
    # Create the Credentials object
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    
    # Open the sheet
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

# --- SMART DATA LOADER (CRITICAL FIX) ---
def normalize_cols(df):
    """
    Automatically fixes column name mismatches from Excel import.
    Example: 'Nsp code' -> 'NSP Code', 'units' -> 'Qty'
    """
    if df.empty: return df
    
    # MAPPING: { "messy name" : "Correct Name" }
    corrections = {
        "nsp code": "NSP Code", "nspcode": "NSP Code", "code": "NSP Code",
        "product name": "Product Name", "productname": "Product Name", "item": "Product Name",
        "units": "Qty", "quantity": "Qty", "qty": "Qty",
        "cost price": "Cost Price", "cp": "Cost Price",
        "selling price": "Selling Price", "sp": "Selling Price", "mrp": "Selling Price",
        "vendor name": "Vendor Name", "vendor": "Vendor Name",
        "invoice no": "Invoice No", "inv": "Invoice No",
        "location": "Location", "loc": "Location"
    }
    
    new_cols = {}
    for c in df.columns:
        clean = str(c).lower().strip()
        if clean in corrections:
            new_cols[c] = corrections[clean]
    
    return df.rename(columns=new_cols)

@st.cache_data(ttl=10)
def load_data(sheet_name):
    try:
        sh = connect_to_gsheet()
        ws = sh.worksheet(sheet_name)
        df = pd.DataFrame(ws.get_all_records())
        return normalize_cols(df)
    except Exception as e:
        # Returns empty DF instead of crashing if sheet is missing cols
        return pd.DataFrame()

def clear_cache():
    load_data.clear()

def save_entry(sheet_name, data_dict):
    try:
        sh = connect_to_gsheet()
        try: ws = sh.worksheet(sheet_name)
        except: 
            # Create sheet if missing
            ws = sh.add_worksheet(sheet_name, 100, 20)
            ws.append_row(list(data_dict.keys()))
            
        headers = ws.row_values(1)
        row_to_append = []
        
        # Match data to headers intelligently
        for h in headers:
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

def update_bal(inv_no, amt_paid):
    """Updates the Paid/Balance columns in Sales sheet"""
    try:
        sh = connect_to_gsheet()
        ws = sh.worksheet("Sales")
        cell = ws.find(str(inv_no))
        if cell:
            headers = ws.row_values(1)
            # Find column indices (1-based for gspread)
            try: p_idx = next(i for i,h in enumerate(headers) if "Paid" in h) + 1
            except: return False
            try: b_idx = next(i for i,h in enumerate(headers) if "Balance" in h) + 1
            except: return False
            
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

# --- INVENTORY ENGINE (THE BRAIN) ---
def get_inv():
    p = load_data("Products")
    if p.empty: return pd.DataFrame()
    
    # Ensure 'NSP Code' exists
    if 'NSP Code' not in p.columns:
        return pd.DataFrame()

    p['Clean'] = p['NSP Code'].astype(str).str.strip().str.lower()
    
    # 1. Initialize Locations with Opening Balance
    for loc in LOCATIONS:
        col = f"Op_{loc.split()[0]}" # Op_Shop, Op_Terrace, Op_Godown
        if col not in p.columns: p[col] = 0
        p[loc] = pd.to_numeric(p[col], errors='coerce').fillna(0)

    # 2. Add PURCHASES (filtered by Location)
    pu = load_data("Purchase")
    if not pu.empty and 'Location' in pu.columns:
        pu['Clean'] = pu['NSP Code'].astype(str).str.strip().str.lower()
        pu['Qty'] = pd.to_numeric(pu['Qty'], errors='coerce').fillna(0)
        
        # Iterate and sum
        for i, row in pu.iterrows():
            if row['Location'] in LOCATIONS:
                p.loc[p['Clean']==row['Clean'], row['Location']] += row['Qty']

    # 3. Subtract SALES (filtered by Location)
    sa = load_data("Sales")
    if not sa.empty and 'Location' in sa.columns:
        sa['Clean'] = sa['NSP Code'].astype(str).str.strip().str.lower()
        sa['Qty'] = pd.to_numeric(sa['Qty'], errors='coerce').fillna(0)
        
        for i, row in sa.iterrows():
            if row['Location'] in LOCATIONS:
                p.loc[p['Clean']==row['Clean'], row['Location']] -= row['Qty']

    # 4. Apply TRANSFERS
    tr = load_data("Transfers")
    if not tr.empty:
        tr['Clean'] = tr['NSP Code'].astype(str).str.strip().str.lower()
        tr['Qty'] = pd.to_numeric(tr['Qty'], errors='coerce').fillna(0)
        
        for i, row in tr.iterrows():
            if row['From_Loc'] in LOCATIONS and row['To_Loc'] in LOCATIONS:
                p.loc[p['Clean']==row['Clean'], row['From_Loc']] -= row['Qty']
                p.loc[p['Clean']==row['Clean'], row['To_Loc']] += row['Qty']

    # 5. Calculate Total
    p['Total Stock'] = p[LOCATIONS].sum(axis=1)
    
    # 6. Fill Cost Price if missing
    if 'Selling Price' in p.columns:
        if 'Cost Price' not in p.columns: p['Cost Price'] = 0.0
        p['Cost Price'] = p.apply(lambda x: (float(x['Selling Price'])/3.3) if (pd.isna(x.get('Cost Price')) or x.get('Cost Price')==0) else x['Cost Price'], axis=1)

    return p

# --- HTML INVOICE ---
def render_invoice(data, bill_type="Non-GST"):
    # GST Logic
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
        <div style="border:1px solid black;padding:20px;font-family:Arial;background:white;color:black;">
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
    # Non-GST Logic
    else:
        rows = ""
        t_amt = 0
        for i, x in enumerate(data['items']):
            qty = float(x.get('Qty',0)); rate = float(x.get('Price',0)); disc = float(x.get('Discount',0))
            tot = qty * (rate - disc)
            t_amt += tot
            rows += f"<tr><td>{i+1}</td><td>{x['Product Name']}</td><td>{x['NSP Code']}</td><td>{qty}</td><td>{rate}</td><td>{disc}</td><td>{tot}</td></tr>"
        
        html = f"""
        <div style="border:1px solid black;padding:20px;font-family:Arial;background:white;color:black;">
            <center><h2>ESTIMATE - SUMEET ENTERPRISES</h2></center>
            <p><b>M/s:</b> {data['cust']} | <b>Phone:</b> {data['phone']}</p>
            <p><b>No:</b> {data['inv']} | <b>Date:</b> {data['date']} | <b>From:</b> {data.get('loc_source','Shop')}</p>
            <table border="1" style="width:100%;border-collapse:collapse;text-align:center;">
                <tr><th>Sn</th><th>Particulars</th><th>Code</th><th>Qty</th><th>Rate</th><th>Disc</th><th>Amount</th></tr>
                {rows}
            </table>
            <h3 style="text-align:right;">Grand Total: {t_amt:,.2f}</h3>
            <p>Paid: {data.get('paid',0)} | Bal: {data.get('bal',0)} | Mode: {data.get('mode','')}</p>
        </div>
        """
    components.html(html, height=800, scrolling=True)

# --- MAIN APP UI ---

if not check_login(): st.stop()
if 'cart' not in st.session_state: st.session_state.cart = []

with st.sidebar:
    st.title("⚡ NEXUS ERP")
    st.caption("v43.0 Cloud | Multi-Location")
    menu = st.radio("Navigation", ["Dashboard", "Sales", "Settle Bookings", "Purchase", "Stock Transfer", "Inventory", "Quotations", "Manufacturing", "Vendor Payments", "Products", "Logs"])
    if st.button("🔄 Refresh Data"): clear_cache(); st.rerun()
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
        st.markdown(f"### 💰 Total Asset Value: ₹{val:,.0f}")
        
        st.markdown("### ⚠️ Low Stock Alert (Shop)")
        low = df[df['Shop'] < 3][['NSP Code','Product Name','Shop','Big Godown']]
        st.dataframe(low, use_container_width=True)
    else:
        st.info("Database connected. No stock data found or columns missing.")

elif menu == "Inventory":
    st.title("📦 Live Inventory")
    df = get_inv()
    if not df.empty:
        # Reorder columns for clarity
        cols = ['NSP Code', 'Product Name', 'Total Stock', 'Shop', 'Terrace Godown', 'Big Godown', 'Selling Price']
        st.dataframe(df[cols], use_container_width=True)

elif menu == "Stock Transfer":
    st.title("🚚 Stock Transfer")
    df = get_inv()
    if not df.empty:
        df['S'] = df['Product Name'] + " | " + df['NSP Code']
        sel = st.selectbox("Select Product", df['S'].unique())
        
        if sel:
            it = df[df['S']==sel].iloc[0]
            st.info(f"Current: Shop [{it['Shop']}] | Terrace [{it['Terrace Godown']}] | Godown [{it['Big Godown']}]")
            
            with st.form("tf_form"):
                c1, c2, c3 = st.columns(3)
                fr = c1.selectbox("From Location", LOCATIONS)
                to = c2.selectbox("To Location", LOCATIONS)
                qty = c3.number_input("Quantity", 1)
                
                if st.form_submit_button("🔄 Transfer"):
                    if fr == to:
                        st.error("Source and Destination are same!")
                    elif it[fr] < qty:
                        st.error(f"Not enough stock in {fr}!")
                    else:
                        save_entry("Transfers", {
                            "Date": datetime.now().strftime("%Y-%m-%d"),
                            "NSP Code": it['NSP Code'], "Product Name": it['Product Name'],
                            "From_Loc": fr, "To_Loc": to, "Qty": qty, "User": st.session_state.user
                        })
                        log_action("Transfer", f"{qty} of {it['NSP Code']} {fr}->{to}")
                        st.success("Transfer Successful!")
                        time.sleep(1)
                        st.rerun()

elif menu == "Sales":
    st.title("🛒 Sales & Billing")
    t1, t2 = st.tabs(["New Invoice", "History"])
    
    with t1:
        loc_s = st.selectbox("📍 Sell From Location", LOCATIONS)
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
                price = c2.number_input("Price", value=float(it.get('Selling Price',0)))
                disc = c3.number_input("Discount", 0.0)
                
                if st.button("Add to Cart"):
                    if av >= qty:
                        st.session_state.cart.append({
                            "NSP Code":it['NSP Code'], "Product Name":it['Product Name'],
                            "Qty":qty, "Price":price, "Discount":disc, "Total":(price-disc)*qty,
                            "Location":loc_s
                        })
                        st.success("Added")
                    else: st.error("Out of Stock in this location!")

        if st.session_state.cart:
            st.divider()
            st.dataframe(pd.DataFrame(st.session_state.cart))
            if st.button("Clear Cart"): st.session_state.cart=[]
            
            gt = sum(x['Total'] for x in st.session_state.cart)
            st.markdown(f"### Total: ₹{gt:,.2f}")
            
            with st.form("checkout"):
                c1, c2 = st.columns(2)
                cust = c1.text_input("Customer Name"); ph = c2.text_input("Phone")
                c3, c4 = st.columns(2)
                mode = c3.selectbox("Mode", ["Cash","UPI","Card","Cheque"]); inv = c4.text_input("Inv No", value=f"INV-{int(time.time())}")
                paid = st.number_input("Amount Paid", value=gt)
                b_type = st.radio("Bill Type", ["Non-GST", "GST"], horizontal=True)
                
                if st.form_submit_button("✅ Generate Invoice"):
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
            st.divider()
            st.markdown("### 🖨️ Print Preview")
            render_invoice(st.session_state.print_data, st.session_state.print_data['bill_type'])
            if st.button("Close Preview"): del st.session_state.print_data; st.rerun()

    with t2:
        st.dataframe(load_data("Sales"), use_container_width=True)
        if st.button("Refresh History"): clear_cache(); st.rerun()

elif menu == "Purchase":
    st.title("🚚 Purchase Entry")
    df = get_inv()
    if not df.empty:
        sel = st.selectbox("Search Product to Restock", df['Product Name'].unique())
        if sel:
            code = df[df['Product Name']==sel].iloc[0]['NSP Code']
            with st.form("buy"):
                l = st.selectbox("Store Where?", LOCATIONS)
                q = st.number_input("Quantity", 1)
                if st.form_submit_button("Save Purchase"):
                    save_entry("Purchase", {
                        "NSP Code":code, "Date":datetime.now().strftime("%Y-%m-%d"),
                        "Qty":q, "Location":l
                    })
                    log_action("Purchase", f"{q} {code} -> {l}")
                    st.success("Stock Added!"); time.sleep(1); st.rerun()

elif menu == "Settle Bookings":
    st.title("💰 Settle Bookings")
    df = load_data("Sales")
    if not df.empty and 'Balance' in df.columns:
        df['Balance'] = pd.to_numeric(df['Balance'], errors='coerce').fillna(0)
        pending = df[df['Balance'] > 0].drop_duplicates(subset=['Invoice No'])
        
        if not pending.empty:
            sel = st.selectbox("Select Pending Invoice", pending['Invoice No'].unique())
            r = pending[pending['Invoice No']==sel].iloc[0]
            st.info(f"Customer: {r.get('Customer Name','')} | Due: ₹{r['Balance']}")
            
            amt = st.number_input("Enter Amount Received", 0.0)
            if st.button("Update Payment"):
                if update_bal(sel, amt):
                    save_entry("Settlements", {"Date":datetime.now().strftime("%Y-%m-%d"),"Invoice No":sel,"Amount Paid":amt})
                    log_action("Settlement", f"{sel}: {amt}")
                    st.success("Updated!"); time.sleep(1); st.rerun()
                else:
                    st.error("Failed to update. Check Sales sheet structure.")
        else:
            st.success("No pending payments found.")

elif menu == "Quotations":
    st.title("📄 Quotations")
    df = get_inv()
    if not df.empty:
        sel = st.selectbox("Item", df['Product Name'].unique(), index=None)
        if sel:
            it = df[df['Product Name']==sel].iloc[0]
            with st.form("q_add"):
                q = st.number_input("Qty",1)
                p = st.number_input("Price", value=float(it.get('Selling Price',0)))
                if st.form_submit_button("Add Item"):
                    st.session_state.cart.append({"NSP Code":it['NSP Code'],"Product Name":it['Product Name'],"Qty":q,"Price":p,"Total":q*p})
                    st.success("Added")
    
    if st.session_state.cart:
        st.dataframe(pd.DataFrame(st.session_state.cart))
        if st.button("Clear"): st.session_state.cart=[]
        with st.form("save_q"):
            cust = st.text_input("Customer Name"); ph = st.text_input("Phone")
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
                st.success(f"Quote Saved: {qid}"); st.rerun()

elif menu == "Manufacturing":
    st.title("🏭 Manufacturing Orders")
    with st.form("mfg"):
        p = st.text_input("Product Name"); c = st.text_input("NSP Code"); q = st.number_input("Qty",1)
        s = st.text_area("Specs/Notes"); d = st.date_input("Deadline")
        if st.form_submit_button("Create Order"):
            save_entry("Manufacturing", {
                "Order No":f"MFG-{int(time.time())}", "Date":datetime.now().strftime("%Y-%m-%d"),
                "Product Name":p, "NSP Code":c, "Qty":q, "Specs":s, "Deadline":d, "Status":"Pending"
            })
            st.success("Order Created")
            st.rerun()
    st.dataframe(load_data("Manufacturing"), use_container_width=True)

elif menu == "Vendor Payments":
    st.title("💸 Vendor Payments")
    with st.form("vp"):
        v = st.text_input("Vendor Name"); a = st.number_input("Amount"); r = st.text_input("Ref/Cheque No"); n = st.text_input("Notes")
        if st.form_submit_button("Record Payment"):
            save_entry("Vendor_Payments", {"Payment ID":f"P-{int(time.time())}","Date":datetime.now().strftime("%Y-%m-%d"),"Vendor Name":v,"Amount":a,"Reference":r,"Notes":n})
            st.success("Saved"); st.rerun()
    st.dataframe(load_data("Vendor_Payments"), use_container_width=True)

elif menu == "Products":
    st.title("📦 Product Master")
    st.dataframe(load_data("Products"), use_container_width=True)
    st.divider()
    st.markdown("### Add New Product")
    with st.form("new_prod"):
        c = st.text_input("NSP Code (Unique)"); n = st.text_input("Product Name"); sp = st.number_input("Selling Price")
        if st.form_submit_button("Add Product"):
            save_entry("Products", {"NSP Code":c,"Product Name":n,"Selling Price":sp, "Cost Price":sp/3.3})
            st.success("Added to Database"); st.rerun()

elif menu == "Logs":
    st.title("📜 System Logs")
    st.dataframe(load_data("Logs"), use_container_width=True)


