import streamlit as st
import pandas as pd
import requests
import json
from concurrent.futures import ThreadPoolExecutor

# --- PAGE CONFIG ---
st.set_page_config(page_title="Bulk API Tool", layout="wide")

# --- HEADER & TOKEN ---
st.title("🚀 Bulk API Enricher")

# Split top section: File Upload (Left) and Token (Right)
col1, col2 = st.columns([1, 1])

with col1:
    uploaded_file = st.file_uploader("1. Upload CSV", type=["csv"])

with col2:
    # UX: Plain text input, no 'Bearer' placeholder needed
    token_input = st.text_input("2. API Token", placeholder="Paste your token here (ey...)", help="We automatically add 'Bearer ' for you.")

# --- CONFIGURATION (Hidden by default for better UX) ---
# We keep the logic in the code, but allow power users to open this if needed.
with st.expander("⚙️ Advanced Configuration (API Mappings)", expanded=False):
    st.info("Edit this only if you need to change endpoints or output fields.")
    
    default_config = {
        "baseUrl": "https://rest.bridgewise.com",
        "concurrency": 5,
        "input": {
            # We will try to auto-detect ID columns, but this is the fallback
            "idColumn": "company_id",
            "keepColumns": ["company_id", "Company_name", "ticker_symbol"]
        },
        "apis": [
            {
                "name": "analysis",
                "urlTemplate": "/companies/{company_id}/analysis?language=en-US",
                "outputs": [
                    { "column": "score", "field": "analysis_score" },
                    { "column": "industry_median", "field": "analysis_score_industry_median" }
                ]
            }
        ],
        "outputColumns": ["company_id", "Company_name", "ticker_symbol", "score", "industry_median"]
    }
    config_input = st.text_area("JSON Logic", value=json.dumps(default_config, indent=2), height=300)

# --- HELPER FUNCTIONS ---

def get_headers(token):
    # UX: Auto-add Bearer
    clean_token = token.strip()
    return {
        "Authorization": f"Bearer {clean_token}",
        "Accept": "application/json"
    }

def load_csv(file):
    try:
        return pd.read_csv(file)
    except UnicodeDecodeError:
        file.seek(0)
        return pd.read_csv(file, encoding='latin1')

def process_row(row, config, headers, debug_mode=False):
    """
    Processes a single row. 
    If debug_mode=True, returns a log of what happened instead of the result row.
    """
    result_row = {}
    debug_log = []

    # 1. Copy original columns defined in config
    for col in config['input'].get('keepColumns', []):
        result_row[col] = row.get(col, "")

    # 2. Loop through APIs
    for api in config['apis']:
        try:
            # URL Construction
            url = config['baseUrl'] + api['urlTemplate']
            
            # Replace placeholders
            for col_name in row.index:
                placeholder = "{" + str(col_name) + "}"
                if placeholder in url:
                    val = str(row[col_name]) if pd.notna(row[col_name]) else ""
                    url = url.replace(placeholder, val)

            if debug_mode:
                debug_log.append(f"**Request:** `GET {url}`")

            # Call API
            resp = requests.get(url, headers=headers)
            
            if debug_mode:
                debug_log.append(f"**Status:** {resp.status_code}")
            
            if resp.status_code == 200:
                data = resp.json()
                
                # Debug: Show the first part of the JSON
                if debug_mode:
                    debug_log.append("**Raw JSON Response:**")
                    debug_log.append(data)

                # Normalize Array vs Object
                if isinstance(data, list):
                    data = data[0] if len(data) > 0 else {}
                
                # Extract Fields
                for output in api['outputs']:
                    field_key = output['field']
                    val = data.get(field_key, None)
                    
                    if debug_mode:
                        debug_log.append(f"👉 Looking for field `'{field_key}'` -> Found: `{val}`")

                    # If val is still None or empty, output is empty string
                    result_row[output['column']] = val if val is not None else ""
            else:
                if debug_mode:
                    debug_log.append(f"❌ API Failed. Response: {resp.text}")
                # Fill empty on failure
                for output in api['outputs']:
                    result_row[output['column']] = ""

        except Exception as e:
            if debug_mode:
                debug_log.append(f"❌ Exception: {str(e)}")
            for output in api['outputs']:
                result_row[output['column']] = ""

    if debug_mode:
        return debug_log
    return result_row

# --- MAIN UI ACTIONS ---

st.divider()

if not uploaded_file or not token_input:
    st.warning("waiting for file and token...")
else:
    # Parse Config & File
    try:
        config = json.loads(config_input)
        df = load_csv(uploaded_file)
        headers = get_headers(token_input)
        
        # --- ACTION 1: TEST BUTTON (DEBUGGING) ---
        st.subheader("3. Validation")
        if st.button("🔍 Test Run (Process 1st Row Only)"):
            st.write("Running diagnostic on the first row of your CSV...")
            
            first_row = df.iloc[0]
            st.markdown(f"**Input Row Data:** `{first_row.to_dict()}`")
            
            # Run with debug mode ON
            logs = process_row(first_row, config, headers, debug_mode=True)
            
            # Display Logs
            for log in logs:
                if isinstance(log, dict) or isinstance(log, list):
                    st.json(log)
                else:
                    st.markdown(log)
                    
            st.success("Test complete. Check the logs above to see if the data was found.")

        # --- ACTION 2: BULK RUN ---
        st.subheader("4. Bulk Processing")
        if st.button("🚀 Start Full Processing", type="primary"):
            
            status_area = st.empty()
            progress_bar = st.progress(0)
            error_log = []
            
            results = []
            total_rows = len(df)
            concurrency = config.get("concurrency", 5)

            status_area.text(f"Starting worker pool with {concurrency} threads...")

            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                rows = df.to_dict('records')
                futures = [executor.submit(process_row, row, config, headers) for row in rows]
                
                for i, future in enumerate(futures):
                    try:
                        res = future.result()
                        results.append(res)
                    except Exception as e:
                        error_log.append(f"Row {i} failed: {str(e)}")
                    
                    # Update UI
                    if i % 10 == 0 or i == total_rows - 1:
                        progress_bar.progress((i + 1) / total_rows)
                        status_area.text(f"Processed {i + 1}/{total_rows}")

            # Finalize
            if error_log:
                st.error(f"Completed with {len(error_log)} errors.")
                with st.expander("View Errors"):
                    for err in error_log[:20]:
                        st.write(err)
            else:
                st.success("Done! No errors reported.")

            # Output DataFrame
            result_df = pd.DataFrame(results)
            
            # Ensure column order
            final_cols = config.get("outputColumns", [])
            # Add missing columns if any
            for col in final_cols:
                if col not in result_df.columns:
                    result_df[col] = ""
            
            # Filter to only requested columns
            if final_cols:
                result_df = result_df[final_cols]

            # Download
            csv_data = result_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Result CSV",
                data=csv_data,
                file_name="enriched_data.csv",
                mime="text/csv"
            )

    except json.JSONDecodeError:
        st.error("Your Configuration JSON is invalid. Please check the 'Advanced Configuration' section.")
    except Exception as e:
        st.error(f"Critical Error: {str(e)}")
