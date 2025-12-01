import streamlit as st
import pandas as pd
import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor

# --- PAGE CONFIG ---
st.set_page_config(page_title="Bulk API Runner", layout="wide")

st.title("🚀 Bulk API Enricher")
st.markdown("Upload a CSV, provide your Config & Token, and enrich your data.")

# --- SIDEBAR: CONFIGURATION ---
with st.sidebar:
    st.header("1. Configuration")
    
    # Default Config Template
    default_config = {
        "baseUrl": "https://rest.bridgewise.com",
        "authHeader": "Authorization",
        "concurrency": 5,
        "input": {
            "idColumn": "company_id",
            "keepColumns": ["company_id", "Company_name"]
        },
        "apis": [
            {
                "name": "analysis",
                "urlTemplate": "/companies/{company_id}/analysis?language=en-US",
                "outputs": [{"column": "score", "field": "analysis_score"}]
            }
        ],
        "outputColumns": ["company_id", "Company_name", "score"]
    }

    token = st.text_input("API Token", type="password", placeholder="Bearer eyJ...")
    config_input = st.text_area("JSON Configuration", value=json.dumps(default_config, indent=2), height=300)

# --- MAIN: FILE UPLOAD ---
st.header("2. Input Data")
uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

# --- PROCESSING ENGINE ---
def call_api(row, config, headers):
    result_row = {}
    
    # Keep original columns
    for col in config['input'].get('keepColumns', []):
        result_row[col] = row.get(col, "")

    # Iterate APIs
    for api in config['apis']:
        try:
            # URL Substitution
            url = config['baseUrl'] + api['urlTemplate']
            for col in row.index:
                placeholder = "{" + str(col) + "}"
                if placeholder in url:
                    val_to_sub = str(row[col]) if pd.notna(row[col]) else ""
                    url = url.replace(placeholder, val_to_sub)
            
            # Request
            resp = requests.get(url, headers=headers)
            
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    data = data[0]
                elif isinstance(data, list):
                    data = {}
                
                # Extract fields
                for output in api['outputs']:
                    field_val = data.get(output['field'], "")
                    result_row[output['column']] = field_val
            else:
                # Fill empty if failed
                for output in api['outputs']:
                    result_row[output['column']] = ""
                    
        except Exception as e:
            # Fill empty on error
             for output in api['outputs']:
                    result_row[output['column']] = ""

    return result_row

# --- RUN BUTTON ---
if st.button("Start Processing", type="primary"):
    if not token or not uploaded_file:
        st.error("Please provide both an API Token and a CSV file.")
    else:
        try:
            # --- FIX: Try loading CSV with UTF-8, fallback to Latin-1 if that fails ---
            try:
                df = pd.read_csv(uploaded_file)
            except UnicodeDecodeError:
                uploaded_file.seek(0) # Reset file pointer
                df = pd.read_csv(uploaded_file, encoding='latin1')
            # ------------------------------------------------------------------------

            config = json.loads(config_input)
            
            headers = {"Accept": "application/json"}
            if config.get("authHeader"):
                headers[config.get("authHeader")] = token

            total_rows = len(df)
            st.info(f"Loaded {total_rows} rows. Processing...")
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            results = []
            
            # Concurrency
            concurrency = config.get("concurrency", 5)
            
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                # Prepare rows as list of dicts
                rows = df.to_dict('records')
                
                # Submit jobs
                futures = [executor.submit(call_api, row, config, headers) for row in rows]
                
                # Collect results
                for i, future in enumerate(futures):
                    results.append(future.result())
                    
                    # Update UI
                    if i % 5 == 0 or i == total_rows - 1:
                        progress = (i + 1) / total_rows
                        progress_bar.progress(progress)
                        status_text.text(f"Processed {i + 1}/{total_rows}")

            # Create DataFrame
            result_df = pd.DataFrame(results)
            
            # Reorder columns based on config
            final_cols = config.get("outputColumns", result_df.columns.tolist())
            # Ensure all columns exist
            for col in final_cols:
                if col not in result_df.columns:
                    result_df[col] = ""
            
            result_df = result_df[final_cols]

            st.success("Processing Complete!")
            
            # Download Button
            csv = result_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download Enriched CSV",
                data=csv,
                file_name="enriched_output.csv",
                mime="text/csv",
            )

        except json.JSONDecodeError:
            st.error("Invalid JSON in Configuration box.")
        except Exception as e:
            st.error(f"An error occurred: {e}")
