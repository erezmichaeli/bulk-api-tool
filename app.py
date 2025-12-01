import streamlit as st
import pandas as pd
import requests
import json
import re
from concurrent.futures import ThreadPoolExecutor

# --- PAGE CONFIG ---
st.set_page_config(page_title="Bulk API Tool", layout="wide")
st.title("🚀 Bulk API Enricher")

# --- SESSION STATE ---
# Store the list of configured API steps
if "api_steps" not in st.session_state:
    st.session_state.api_steps = []

# Store the loaded Swagger definition
if "swagger_endpoints" not in st.session_state:
    st.session_state.swagger_endpoints = {}

# --- HELPER FUNCTIONS ---

def get_headers(token):
    return {
        "Authorization": f"Bearer {token.strip()}",
        "Accept": "application/json"
    }

def load_csv(file):
    try:
        file.seek(0)
        return pd.read_csv(file)
    except:
        file.seek(0)
        return pd.read_csv(file, encoding='latin1')

def parse_swagger(file):
    """
    Parses a Swagger/OpenAPI JSON file to extract GET endpoints and their parameters.
    """
    try:
        data = json.load(file)
        endpoints = {}
        
        paths = data.get('paths', {})
        for path, methods in paths.items():
            if 'get' in methods:
                details = methods['get']
                summary = details.get('summary', details.get('operationId', path))
                
                # Extract parameters (path variables like {id})
                params = []
                # Check path level parameters
                if 'parameters' in methods:
                    for p in methods['parameters']:
                        if p.get('in') == 'path':
                            params.append(p['name'])
                # Check method level parameters
                if 'parameters' in details:
                    for p in details['parameters']:
                        if p.get('in') == 'path':
                            params.append(p['name'])
                
                # Find distinct path params from URL string if not explicitly listed
                matches = re.findall(r"\{(.*?)\}", path)
                for m in matches:
                    if m not in params:
                        params.append(m)

                label = f"{summary} ({path})"
                endpoints[label] = {
                    "url": path,
                    "params": list(set(params)), # unique
                    "description": details.get('description', "")
                }
        return endpoints
    except Exception as e:
        st.error(f"Error parsing Swagger file: {e}")
        return {}

def process_single_row(row, api_steps, base_url, headers, debug=False):
    """
    Processes a single row through ALL configured API steps.
    """
    # Normalize row to dict if it's a pandas Series
    if hasattr(row, 'to_dict'):
        row_data = row.to_dict()
    else:
        row_data = row.copy()

    debug_log = []
    
    # Iterate through each configured API step
    for step_index, step in enumerate(api_steps):
        try:
            # 1. Build URL
            # We take the template and replace {param} with the value from the mapped CSV column
            url_path = step['url_template']
            
            # Map parameters
            # param_map looks like: {'company_id': 'CSV_Column_A'}
            for param_name, csv_col in step['param_map'].items():
                if csv_col and csv_col in row_data:
                    val = str(row_data[csv_col]) if pd.notna(row_data[csv_col]) else ""
                    url_path = url_path.replace(f"{{{param_name}}}", val)
            
            full_url = base_url.rstrip('/') + '/' + url_path.lstrip('/')

            if debug:
                debug_log.append(f"**Step {step_index+1} ({step['name']}):** `GET {full_url}`")

            # 2. Call API
            resp = requests.get(full_url, headers=headers)
            
            if debug:
                debug_log.append(f"Status: `{resp.status_code}`")

            if resp.status_code == 200:
                data = resp.json()
                
                # Handle Array responses
                if isinstance(data, list):
                    data = data[0] if len(data) > 0 else {}
                
                if debug:
                    json_preview = json.dumps(data, indent=2)[:500]
                    debug_log.append(f"```json\n{json_preview}\n```")

                # 3. Extract Outputs
                # output_map is a list of dicts: [{'json_field': 'score', 'csv_column': 'Score'}]
                for mapping in step['output_map']:
                    j_field = mapping['json_field']
                    c_col = mapping['csv_column']
                    
                    if j_field and c_col:
                        val = data.get(j_field, "")
                        # Save to row_data so next steps can potentially use it, and for final CSV
                        row_data[c_col] = val
                        if debug:
                            debug_log.append(f"✅ Extracted `{j_field}` -> `{val}`")
            else:
                if debug:
                    debug_log.append(f"❌ Failed: {resp.text}")
                # Fill empties
                for mapping in step['output_map']:
                    if mapping['csv_column']:
                        row_data[mapping['csv_column']] = ""

        except Exception as e:
            if debug:
                debug_log.append(f"❌ Exception: {str(e)}")
            for mapping in step['output_map']:
                if mapping['csv_column']:
                    row_data[mapping['csv_column']] = ""

    if debug:
        return debug_log
    return row_data

# --- UI LAYOUT ---

with st.sidebar:
    st.header("1. Setup")
    base_url = st.text_input("Base URL", value="https://rest.bridgewise.com")
    token_input = st.text_input("Access Token", type="password")
    
    st.divider()
    st.subheader("Optional: Swagger")
    swagger_file = st.file_uploader("Upload swagger.json to auto-fill endpoints", type=['json'])
    if swagger_file:
        st.session_state.swagger_endpoints = parse_swagger(swagger_file)
        st.success(f"Loaded {len(st.session_state.swagger_endpoints)} endpoints.")

col_main, col_preview = st.columns([2, 1])

with col_main:
    st.header("2. Input Data")
    uploaded_file = st.file_uploader("Upload CSV File", type=["csv"])

if uploaded_file and token_input:
    df = load_csv(uploaded_file)
    csv_headers = df.columns.tolist()
    
    st.divider()
    st.header("3. Configure API Steps")
    st.info("You can add multiple API calls. They will run in order for every row.")

    # --- STEP CONFIGURATOR ---
    with st.expander("➕ Add New API Step", expanded=True):
        
        # Endpoint Selection
        if st.session_state.swagger_endpoints:
            endpoint_options = list(st.session_state.swagger_endpoints.keys())
            selected_label = st.selectbox("Select Endpoint", options=["Custom"] + endpoint_options)
            
            if selected_label != "Custom":
                ep_data = st.session_state.swagger_endpoints[selected_label]
                url_template = ep_data['url']
                required_params = ep_data['params']
            else:
                url_template = st.text_input("URL Path (e.g. /companies/{id})", value="/companies/{id}")
                required_params = [m.group(1) for m in re.finditer(r"\{(.*?)\}", url_template)]
        else:
            url_template = st.text_input("URL Path (e.g. /companies/{id}/analysis)", value="/companies/{id}/analysis")
            required_params = [m.group(1) for m in re.finditer(r"\{(.*?)\}", url_template)]

        st.caption(f"Path: `{url_template}`")

        # Parameter Mapping
        st.subheader("Map URL Parameters")
        param_map = {}
        if required_params:
            cols = st.columns(len(required_params))
            for i, p in enumerate(required_params):
                with cols[i % len(cols)]:
                    # Try to find a matching header automatically
                    default_idx = 0
                    if p in csv_headers:
                        default_idx = csv_headers.index(p)
                    elif "company_id" in csv_headers:
                         default_idx = csv_headers.index("company_id")
                    
                    param_map[p] = st.selectbox(f"Value for '{{{p}}}'", options=csv_headers, index=default_idx, key=f"param_{p}")
        else:
            st.write("No parameters detected in URL.")

        # Output Mapping
        st.subheader("Define Outputs")
        # Initialize default dataframe for editor
        default_data = [{"json_field": "score", "csv_column": "analysis_score"}]
        edited_df = st.data_editor(
            default_data, 
            num_rows="dynamic",
            column_config={
                "json_field": st.column_config.TextColumn("JSON Field (API)", required=True),
                "csv_column": st.column_config.TextColumn("New Column Name (CSV)", required=True)
            },
            key="editor_new_step"
        )

        if st.button("Save Step"):
            new_step = {
                "name": selected_label if st.session_state.swagger_endpoints else "Custom API",
                "url_template": url_template,
                "param_map": param_map,
                "output_map": edited_df
            }
            st.session_state.api_steps.append(new_step)
            st.success("Step added! You can add more or scroll down to run.")

    # --- SHOW CONFIGURED STEPS ---
    if st.session_state.api_steps:
        st.subheader("Pipeline Review")
        for i, step in enumerate(st.session_state.api_steps):
            with st.expander(f"Step {i+1}: {step['name']} ({step['url_template']})"):
                st.write("**Parameters:**", step['param_map'])
                st.write("**Outputs:**", pd.DataFrame(step['output_map']))
                if st.button(f"Remove Step {i+1}", key=f"rm_{i}"):
                    st.session_state.api_steps.pop(i)
                    st.rerun()

    # --- EXECUTION ---
    st.divider()
    st.header("4. Execution")
    
    col_act1, col_act2 = st.columns([1, 3])
    
    with col_act1:
        if st.button("🔍 Test 1 Row"):
            if not st.session_state.api_steps:
                st.error("Please add at least one API step above.")
            else:
                first_row = df.iloc[0]
                logs = process_single_row(
                    first_row, 
                    st.session_state.api_steps, 
                    base_url, 
                    get_headers(token_input), 
                    debug=True
                )
                with st.container():
                    st.write("### Test Results")
                    for log in logs:
                        st.markdown(log)

    with col_act2:
        if st.button("🚀 Process Full File", type="primary"):
            if not st.session_state.api_steps:
                st.error("Add API steps first.")
            else:
                total_rows = len(df)
                progress_bar = st.progress(0)
                status_text = st.empty()
                results = []
                
                with ThreadPoolExecutor(max_workers=5) as executor:
                    # Convert to dict records for processing
                    rows_input = df.to_dict('records')
                    
                    futures = [
                        executor.submit(
                            process_single_row, 
                            row, 
                            st.session_state.api_steps, 
                            base_url, 
                            get_headers(token_input), 
                            False
                        ) for row in rows_input
                    ]
                    
                    for i, f in enumerate(futures):
                        try:
                            res = f.result()
                            results.append(res)
                        except Exception as e:
                            # Fallback if critical failure, append original
                            results.append(rows_input[i])
                            st.error(f"Row {i} failed: {e}")

                        if i % 10 == 0:
                            progress_bar.progress((i + 1) / total_rows)
                            status_text.text(f"Processed {i + 1}/{total_rows}")
                
                progress_bar.progress(100)
                status_text.text("Done!")
                
                final_df = pd.DataFrame(results)
                st.success(f"Completed! {len(final_df)} rows enriched.")
                
                csv_data = final_df.to_csv(index=False).encode('utf-8')
                st.download_button("📥 Download Result", csv_data, "enriched.csv", "text/csv")

else:
    st.info("Upload CSV and provide Token to start.")
