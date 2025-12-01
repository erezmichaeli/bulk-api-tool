import streamlit as st
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor

# --- PAGE CONFIG ---
st.set_page_config(page_title="Bulk API Tool", layout="wide")
st.title("🚀 Bulk API Enricher")

# --- SESSION STATE INITIALIZATION ---
# We store the output mappings in session state so they persist between clicks
if "output_mappings" not in st.session_state:
    # Default example mappings
    st.session_state.output_mappings = [
        {"json_field": "analysis_score", "csv_column_name": "score"},
        {"json_field": "analysis_score_industry_median", "csv_column_name": "industry_median"},
    ]

# --- HELPER FUNCTIONS ---
def get_headers(token):
    return {
        "Authorization": f"Bearer {token.strip()}",
        "Accept": "application/json"
    }

def load_csv_headers(file):
    """Reads just the first row to get headers safely"""
    try:
        file.seek(0)
        return pd.read_csv(file, nrows=0).columns.tolist()
    except:
        file.seek(0)
        return pd.read_csv(file, nrows=0, encoding='latin1').columns.tolist()

def load_full_csv(file):
    """Reads the full CSV"""
    try:
        file.seek(0)
        return pd.read_csv(file)
    except:
        file.seek(0)
        return pd.read_csv(file, encoding='latin1')

def resolve_url(base_url, template, row, id_col_name):
    """
    Constructs the URL. 
    It replaces {id} in the template with the value from the selected ID column.
    """
    # Combine Base + Template
    full_url = base_url.rstrip('/') + '/' + template.lstrip('/')
    
    # Get the ID value from the row using the mapped column name
    id_val = str(row[id_col_name]) if pd.notna(row[id_col_name]) else ""
    
    # Replace standard placeholder {id} with the actual value
    # We allow the user to use {id} in the UI as a generic placeholder
    target_url = full_url.replace("{id}", id_val)
    
    return target_url

def process_single_row(row, id_col_name, base_url, url_template, mappings, headers, debug=False):
    debug_log = []
    result_row = row.to_dict() # Start with existing data
    
    try:
        # 1. Build URL
        url = resolve_url(base_url, url_template, row, id_col_name)
        
        if debug:
            debug_log.append(f"**Request:** `GET {url}`")

        # 2. Call API
        resp = requests.get(url, headers=headers)
        
        if debug:
            debug_log.append(f"**Status:** {resp.status_code}")
            
        if resp.status_code == 200:
            data = resp.json()
            
            # Handle Array responses (take first item)
            if isinstance(data, list):
                data = data[0] if len(data) > 0 else {}
            
            if debug:
                debug_log.append("**Raw JSON Response (First 500 chars):**")
                debug_log.append(str(json.dumps(data))[:500] + "...")

            # 3. Extract Fields based on table mappings
            for map_row in mappings:
                j_field = map_row["json_field"]
                c_name = map_row["csv_column_name"]
                
                # Check if user provided empty mapping
                if j_field and c_name:
                    val = data.get(j_field, "")
                    result_row[c_name] = val
                    if debug:
                        debug_log.append(f"✅ Found `{j_field}`: {val} -> Saved to `{c_name}`")
        else:
            if debug:
                debug_log.append(f"❌ API Error: {resp.text}")
            
            # Fill empty on error
            for map_row in mappings:
                if map_row["csv_column_name"]:
                    result_row[map_row["csv_column_name"]] = ""

    except Exception as e:
        if debug:
            debug_log.append(f"❌ Exception: {str(e)}")
        for map_row in mappings:
            if map_row["csv_column_name"]:
                result_row[map_row["csv_column_name"]] = ""
                
    if debug:
        return debug_log
    return result_row

# --- UI LAYOUT ---

# 1. TOP BAR: INPUTS
col1, col2 = st.columns([1, 1])
with col1:
    uploaded_file = st.file_uploader("1. Upload CSV", type=["csv"])
with col2:
    token_input = st.text_input("2. API Token", placeholder="Paste token (no 'Bearer' prefix needed)", type="password")

# 2. DYNAMIC CONFIGURATION (Only shows if file is uploaded)
if uploaded_file is not None:
    csv_headers = load_csv_headers(uploaded_file)
    
    st.divider()
    st.subheader("3. Configuration")
    
    c1, c2, c3 = st.columns([1, 1, 1])
    
    with c1:
        # Dynamic Dropdown from CSV
        id_column = st.selectbox("Which CSV column contains the ID?", options=csv_headers, index=0)
    
    with c2:
        base_url = st.text_input("Base URL", value="https://rest.bridgewise.com")
        
    with c3:
        # We tell the user to use {id} as the placeholder
        url_template = st.text_input("Endpoint Path", value="/companies/{id}/analysis?language=en-US", help="Use {id} where the Company ID should go.")

    st.markdown("#### 4. Output Fields Mapping")
    st.caption("Add rows below to extract more fields from the API. The 'JSON Field' is exactly what the API returns. The 'CSV Column' is what you want to name it in your file.")
    
    # EDITABLE TABLE
    # Users can add/delete rows here easily
    mapping_df = pd.DataFrame(st.session_state.output_mappings)
    edited_mapping = st.data_editor(
        mapping_df, 
        num_rows="dynamic", 
        use_container_width=True,
        column_config={
            "json_field": st.column_config.TextColumn("JSON Field Name (API)", required=True),
            "csv_column_name": st.column_config.TextColumn("Output Column Name (CSV)", required=True)
        }
    )
    
    # Convert back to list of dicts for processing
    current_mappings = edited_mapping.to_dict('records')

    # --- ACTIONS ---
    st.divider()
    st.subheader("5. Execution")

    act_col1, act_col2 = st.columns([1, 4])
    
    with act_col1:
        test_btn = st.button("🔍 Test 1 Row")
    with act_col2:
        run_btn = st.button("🚀 Process Full File", type="primary")

    # LOGIC: TEST
    if test_btn:
        if not token_input:
            st.error("Please enter an API Token first.")
        else:
            df_preview = load_full_csv(uploaded_file)
            first_row = df_preview.iloc[0]
            st.info(f"Testing with ID: {first_row[id_column]}")
            
            logs = process_single_row(
                first_row, 
                id_column, 
                base_url, 
                url_template, 
                current_mappings, 
                get_headers(token_input), 
                debug=True
            )
            
            with st.expander("View Test Logs", expanded=True):
                for l in logs:
                    st.markdown(l)

    # LOGIC: RUN
    if run_btn:
        if not token_input:
            st.error("Please enter an API Token first.")
        else:
            df = load_full_csv(uploaded_file)
            total_rows = len(df)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            results = []
            headers = get_headers(token_input)
            
            # Using ThreadPool for speed
            with ThreadPoolExecutor(max_workers=5) as executor:
                rows_list = df.to_dict('records')
                futures = []
                
                for row in rows_list:
                    futures.append(executor.submit(
                        process_single_row, 
                        row, 
                        id_column, 
                        base_url, 
                        url_template, 
                        current_mappings, 
                        headers, 
                        False # No debug
                    ))
                
                for i, f in enumerate(futures):
                    results.append(f.result())
                    if i % 10 == 0:
                        progress_bar.progress((i + 1) / total_rows)
                        status_text.text(f"Processed {i + 1}/{total_rows}")
            
            progress_bar.progress(100)
            status_text.text("Done!")
            
            # Create Final DataFrame
            final_df = pd.DataFrame(results)
            
            # Reorder: Put the original headers first, then the new ones
            original_cols = csv_headers
            new_cols = [m['csv_column_name'] for m in current_mappings if m['csv_column_name']]
            
            # Combine unique columns while preserving order
            all_cols = []
            seen = set()
            for c in original_cols + new_cols:
                if c not in seen:
                    all_cols.append(c)
                    seen.add(c)
            
            final_df = final_df[all_cols]
            
            st.success(f"Processing complete! Enriched {len(final_df)} rows.")
            
            csv_data = final_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Result CSV",
                data=csv_data,
                file_name="enriched_output.csv",
                mime="text/csv"
            )

else:
    st.info("👆 Please upload a CSV file to start configuring the tool.")
