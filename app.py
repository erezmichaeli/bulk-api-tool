import streamlit as st
import pandas as pd
import requests
import json  # <--- FIXED: Added this import so the logs work
from concurrent.futures import ThreadPoolExecutor

# --- PAGE CONFIG ---
st.set_page_config(page_title="Bulk API Tool", layout="wide")
st.title("🚀 Bulk API Enricher")

# --- SESSION STATE INITIALIZATION ---
if "output_mappings" not in st.session_state:
    # Default example mappings updated for Fundamental Analysis
    st.session_state.output_mappings = [
        {"json_field": "score", "csv_column_name": "fundamental_score"},
        {"json_field": "conviction", "csv_column_name": "conviction"},
    ]

# --- HELPER FUNCTIONS ---
def get_headers(token):
    return {
        "Authorization": f"Bearer {token.strip()}",
        "Accept": "application/json"
    }

def load_csv_headers(file):
    try:
        file.seek(0)
        return pd.read_csv(file, nrows=0).columns.tolist()
    except:
        file.seek(0)
        return pd.read_csv(file, nrows=0, encoding='latin1').columns.tolist()

def load_full_csv(file):
    try:
        file.seek(0)
        return pd.read_csv(file)
    except:
        file.seek(0)
        return pd.read_csv(file, encoding='latin1')

def resolve_url(base_url, template, row, id_col_name):
    # Combine Base + Template
    full_url = base_url.rstrip('/') + '/' + template.lstrip('/')
    
    # Get the ID value
    id_val = str(row[id_col_name]) if pd.notna(row[id_col_name]) else ""
    
    # Replace {id} placeholder
    target_url = full_url.replace("{id}", id_val)
    return target_url

def process_single_row(row, id_col_name, base_url, url_template, mappings, headers, debug=False):
    debug_log = []
    result_row = row.to_dict() 
    
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
            
            # Handle Array responses
            if isinstance(data, list):
                data = data[0] if len(data) > 0 else {}
            
            if debug:
                # Pretty print JSON for the logs
                json_str = json.dumps(data, indent=2)
                debug_log.append("**Raw JSON Response (First 1000 chars):**")
                debug_log.append(f"```json\n{json_str[:1000]}\n```")

            # 3. Extract Fields
            for map_row in mappings:
                j_field = map_row["json_field"]
                c_name = map_row["csv_column_name"]
                
                if j_field and c_name:
                    val = data.get(j_field, "")
                    result_row[c_name] = val
                    if debug:
                        debug_log.append(f"✅ Found `{j_field}`: `{val}` -> Saved to `{c_name}`")
        else:
            if debug:
                debug_log.append(f"❌ API Error: {resp.text}")
            
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

col1, col2 = st.columns([1, 1])
with col1:
    uploaded_file = st.file_uploader("1. Upload CSV", type=["csv"])
with col2:
    # UPDATED: No password type (visible), label changed to Access Token
    token_input = st.text_input("2. Access Token", placeholder="Paste token here (no 'Bearer' prefix needed)")

if uploaded_file is not None:
    csv_headers = load_csv_headers(uploaded_file)
    
    st.divider()
    st.subheader("3. Configuration")
    
    c1, c2, c3 = st.columns([1, 1, 1])
    
    with c1:
        id_column = st.selectbox("Which CSV column contains the ID?", options=csv_headers, index=0)
    
    with c2:
        base_url = st.text_input("Base URL", value="https://rest.bridgewise.com")
        
    with c3:
        # UPDATED: Default example to fundamental-analysis
        url_template = st.text_input("Endpoint Path", value="/companies/{id}/fundamental-analysis?language=en-US", help="Use {id} where the Company ID should go.")

    st.markdown("#### 4. Output Fields Mapping")
    st.caption("Add rows below to extract fields. 'JSON Field' is the key in the API response. 'CSV Column' is the header in your output file.")
    
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
    
    current_mappings = edited_mapping.to_dict('records')

    st.divider()
    st.subheader("5. Execution")

    act_col1, act_col2 = st.columns([1, 4])
    
    with act_col1:
        test_btn = st.button("🔍 Test 1 Row")
    with act_col2:
        run_btn = st.button("🚀 Process Full File", type="primary")

    # TEST LOGIC
    if test_btn:
        if not token_input:
            st.error("Please enter an Access Token first.")
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

    # RUN LOGIC
    if run_btn:
        if not token_input:
            st.error("Please enter an Access Token first.")
        else:
            df = load_full_csv(uploaded_file)
            total_rows = len(df)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            results = []
            headers = get_headers(token_input)
            
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
                        False 
                    ))
                
                for i, f in enumerate(futures):
                    results.append(f.result())
                    if i % 10 == 0:
                        progress_bar.progress((i + 1) / total_rows)
                        status_text.text(f"Processed {i + 1}/{total_rows}")
            
            progress_bar.progress(100)
            status_text.text("Done!")
            
            final_df = pd.DataFrame(results)
            
            # Reorder columns
            original_cols = csv_headers
            new_cols = [m['csv_column_name'] for m in current_mappings if m['csv_column_name']]
            
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
    st.info("👆 Please upload a CSV file to start.")
