# app.py
import io
import re
import traceback
from datetime import datetime
from typing import List

import pdfplumber
import pandas as pd
import streamlit as st
from pypdf import PdfReader

# --------------------------- 
# User roles (same as original) 
# --------------------------- 
USER_ROLES = {
    "admin": {"password": "adminpass", "role": "admin"},
    "user": {"password": "userpass", "role": "user"}
}

# --------------------------- 
# Helper functions (ported & adapted) 
# --------------------------- 

def _extract_consumer_info(consumer_no):
    """Extracts structured keys from a 'Consumer No' string."""
    area_code, consumer_code, category = "", "", ""
    if isinstance(consumer_no, str):
        cleaned_consumer_no = re.sub(r'[\r\n\s]+', '', consumer_no)
        if '/' in cleaned_consumer_no:
            parts = cleaned_consumer_no.split('/')
            if len(parts) >= 1:
                area_code = parts[0][:3]
            if len(parts) >= 2:
                consumer_code = parts[1]
            if len(parts) >= 3:
                category = parts[2][-1] if len(parts[2]) > 0 else ""
    
    primary_key = str(area_code) + str(consumer_code) + str(category)
    return area_code, consumer_code, category, primary_key

def read_excel_or_csv(uploaded_file):
    """Read uploaded file (BytesIO) as DataFrame or dict of DataFrames for Excel."""
    try:
        filename = uploaded_file.name
    except Exception:
        filename = "uploaded_file"
    if filename.lower().endswith(('.xls', '.xlsx')):
        # return dict of dataframes for multi-sheet; streamlit provides BytesIO-like file
        try:
            xls = pd.read_excel(uploaded_file, sheet_name=None, engine='openpyxl')
            return xls
        except Exception as e:
            st.error(f"Failed to read Excel file: {e}")
            return None
    elif filename.lower().endswith('.csv'):
        try:
            df = pd.read_csv(uploaded_file)
            return df
        except Exception as e:
            st.error(f"Failed to read CSV file: {e}")
            return None
    else:
        st.error("Unsupported file type. Upload .xlsx, .xls or .csv")
        return None

def check_columns_exist(df, required_columns):
    if df is None:
        return False
    missing = [c for c in required_columns if c not in df.columns]
    return len(missing) == 0

def find_sheet_by_columns(xls_dict, required_columns):
    """Given sheet_name->df dict, find first sheet that contains required_columns."""
    if not isinstance(xls_dict, dict):
        # single df
        if isinstance(xls_dict, pd.DataFrame) and check_columns_exist(xls_dict, required_columns):
            return xls_dict, "MainData"
        return None, None
    for name, df in xls_dict.items():
        if check_columns_exist(df, required_columns):
            return df, name
    return None, None


def build_template_bytes(columns, sheet_name="Template"):
    """
    Build an in-memory Excel file (bytes) containing only the provided columns
    as an empty template sheet. Useful for download buttons that provide a
    template for users to fill in.
    """
    template_df = pd.DataFrame(columns=columns)
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        template_df.to_excel(writer, index=False, sheet_name=sheet_name)
    return buffer.getvalue()


# ---------------------------
# PDF parsing using pdfplumber + pypdf for route detection
# ---------------------------

def extract_route_from_pdf_bytes(file_bytes):
    """Use pypdf to extract text and find Route No."""
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
        if len(reader.pages) > 0:
            first_page_text = reader.pages[0].extract_text() or ""
            route_match = re.search(r"Route\s*No\s*[:\-]?\s*(\d+)", first_page_text, re.IGNORECASE)
            if route_match:
                return route_match.group(1)
    except Exception:
        pass
    return None

def pdfplumber_extract_tables(file_bytes) -> pd.DataFrame:
    """
    Extract consumer records from PDF text by scanning line windows for
    consumer numbers, dates, and readings, returning a DataFrame of the results.
    """
    consumer_pattern = re.compile(r"([A-Z]{3})/(\d{1,6})/([NDSI])")
    date_pattern = re.compile(r"\b\d{2}[-/]\d{2}[-/]\d{4}\b")
    number_pattern = re.compile(r"-?\d{1,7}(?:\.\d+)?")
    labeled_reading_pattern = re.compile(
        r"(?:Prev(?:ious)?|Last)\s*(?:Reading|Rdg)\s*[:\-]?\s*(\d{1,6}(?:\.\d+)?)",
        re.IGNORECASE,
    )

    records = []

    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                lines = text.splitlines()
                for i, line in enumerate(lines):
                    cons_match = consumer_pattern.search(line)
                    look_index = i
                    if not cons_match:
                        window = " ".join(lines[i:i + 2])
                        cons_match = consumer_pattern.search(window)
                        if not cons_match:
                            continue

                    area, code, cat = cons_match.groups()
                    consumer_no = cons_match.group(0)
                    primary_key = f"{area}{code}{cat}"

                    prev_date = ""
                    prev_reading = ""

                    segment = " ".join(lines[look_index: look_index + 3])
                    date_match = date_pattern.search(segment)
                    if not date_match:
                        extended_segment = " ".join(lines[look_index: look_index + 6])
                        date_match = date_pattern.search(extended_segment)
                        if date_match:
                            segment = extended_segment

                    if date_match:
                        prev_date = date_match.group(0)
                        after_start = date_match.end()
                        tail = segment[after_start: after_start + 60]
                        num_match = number_pattern.search(tail)
                        if num_match:
                            prev_reading = num_match.group(0)

                    if not prev_reading:
                        labeled = labeled_reading_pattern.search(segment)
                        if labeled:
                            prev_reading = labeled.group(1)

                    if not prev_reading:
                        for reading_match in re.finditer(r"(\d{1,6}(?:\.\d+)?)", segment):
                            ctx_start = max(0, reading_match.start() - 30)
                            context = segment[ctx_start:reading_match.start()].lower()
                            skip_keywords = [
                                "meter", "meter no", "meter number", "mtr no", "consumer",
                                "consumer no", "consumer no.", "account", "acct", "phone",
                                "mobile", "tel", "telephone", "fax"
                            ]
                            if any(keyword in context for keyword in skip_keywords):
                                continue
                            if consumer_no and reading_match.group(1) in consumer_no:
                                continue
                            prev_reading = reading_match.group(1)
                            break

                    records.append({
                        "Consumer No.": consumer_no,
                        "Area code": area,
                        "Consumer code": code,
                        "category": cat,
                        "Primary key": primary_key,
                        "Previous Reading": prev_reading.strip(),
                        "Previous Reading Date": prev_date,
                    })
    except Exception as e:
        st.warning(f"pdfplumber extraction problem: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    if df.empty:
        return df

    df = df.drop_duplicates(subset=["Primary key"]).reset_index(drop=True)

    for placeholder_col in [
        "SL. No.", "Route number", "Phone No.", "Meter Number",
        "Arrears", "Current Reading", "Payable", "Remarks"
    ]:
        if placeholder_col not in df.columns:
            df[placeholder_col] = ""

    return df

def normalize_pdf_table(df: pd.DataFrame, route_no=None) -> pd.DataFrame:
    """
    Attempt to rename columns to the expected names in your original program,
    extract consumer splits, and final ordering.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # cleanup column names
    new_cols = []
    col_map = {
        r"SL\.?\s*No\.?": "SL. No.",
        r"Route\s*(No\.?|Number)": "Route number",
        r"Consumer\s*No\.?": "Consumer No.",
        r"Phone\s*No\.?": "Phone No.",
        r"Meter\s*Number": "Meter Number",
        r"(Prev(?:ious)?|Last)\s*(Read(?:ing)?|Rdg)\s*(Date|Dt\.?)": "Previous Reading Date",
        r"Previous\s*Reading\s*Date": "Previous Reading Date",
        r"(Prev(?:ious)?|Last)\s*(Read(?:ing)?|Rdg)(?!\s*(Date|Dt\.?))": "Previous Reading",
        r"Previous\s*Reading": "Previous Reading",
        r"Arrears": "Arrears",
        r"Current\s*Reading": "Current Reading",
        r"(Amount|Payable)": "Payable",
        r"Remarks": "Remarks",
        r"Bill\s*Issued": "Remarks"
    }
    for col in df.columns:
        c = str(col)
        mapped = None
        for pat, name in col_map.items():
            if re.search(pat, c, re.IGNORECASE):
                mapped = name
                break
        new_cols.append(mapped if mapped else c.strip())
    df.columns = new_cols

    # ensure expected final columns exist
    final_columns_order = [
        "SL. No.", "Route number", "Consumer No.", "Area code", "Consumer code",
        "category", "Primary key", "Phone No.", "Meter Number",
        "Previous Reading Date", "Previous Reading", "Arrears",
        "Current Reading", "Payable", "Remarks"
    ]
    # Clean Consumer No.
    if 'Consumer No.' in df.columns:
        df['Consumer No.'] = df['Consumer No.'].astype(str).str.strip().str.replace(r'[\r\n\s]+', '', regex=True)
        split_consumer = df['Consumer No.'].str.split(r'/', expand=True, n=2)
        df['Area code'] = split_consumer[0].fillna('')
        df['Consumer code'] = split_consumer[1].fillna('') if 1 in split_consumer.columns else ''
        df['category'] = split_consumer[2].fillna('') if 2 in split_consumer.columns else ''
        df['Primary key'] = df['Area code'].astype(str) + df['Consumer code'].astype(str) + df['category'].astype(str)
    else:
        # fallback: attempt to find a consumer column name
        possible = [c for c in df.columns if 'consumer' in str(c).lower()]
        if possible:
            df.rename(columns={possible[0]: 'Consumer No.'}, inplace=True)
            df['Consumer No.'] = df['Consumer No.'].astype(str).str.strip().str.replace(r'[\r\n\s]+', '', regex=True)
            split_consumer = df['Consumer No.'].str.split(r'/', expand=True, n=2)
            df['Area code'] = split_consumer[0].fillna('')
            df['Consumer code'] = split_consumer[1].fillna('') if 1 in split_consumer.columns else ''
            df['category'] = split_consumer[2].fillna('') if 2 in split_consumer.columns else ''
            df['Primary key'] = df['Area code'].astype(str) + df['Consumer code'].astype(str) + df['category'].astype(str)
        else:
            # create empty columns
            df['Area code'] = ''
            df['Consumer code'] = ''
            df['category'] = ''
            df['Primary key'] = ''

    df['Route number'] = route_no if route_no is not None else df.get('Route number', '')

    # ensure all final columns present
    for col in final_columns_order:
        if col not in df.columns:
            df[col] = ''
    df = df[final_columns_order]
    # remove rows that look like headers repeated
    df = df[df['SL. No.'].apply(lambda x: str(x).strip().lower() not in ['sl. no.', 'sl no', 's.no', 's.no.'])]
    return df

# --------------------------- 
# BPL processing (ported)
# --------------------------- 

REQUIRED_BPL_COLUMNS = ['Sl No', 'Consumer No', 'Present Status', 'Effect From', 'Effect To', 'Last Updated On']
REQUIRED_AREAR_COLUMNS_AREAR_LIST = ['Sl.No','Consumer No','Address','Phone','Route', 'Last Reading Date','Last Pay Date','Last Amount Paid','Arrears','Disconn. Date']
REQUIRED_AREAR_COLUMNS_READER_LIST = ['RouteCode', 'Meter reader']

def process_bpl_data_from_df(bpl_sheet_df):
    try:
        bpl_sheet = bpl_sheet_df.dropna(how='all').copy()
        if not check_columns_exist(bpl_sheet, REQUIRED_BPL_COLUMNS):
            st.error("BPL sheet does not have required columns.")
            return None
        bpl_sheet['Effect From'] = pd.to_datetime(bpl_sheet['Effect From'], errors='coerce')
        bpl_sheet['Effect To'] = pd.to_datetime(bpl_sheet['Effect To'], errors='coerce')
        bpl_sheet['Last Updated On'] = pd.to_datetime(bpl_sheet['Last Updated On'], errors='coerce')
        bpl_sheet['Year'] = bpl_sheet['Effect From'].dt.year.fillna(-1).astype(int)
        bpl_sheet['Month'] = bpl_sheet['Effect From'].dt.month.fillna(-1).astype(int)
        if 'Consumer No' in bpl_sheet.columns:
            bpl_sheet['Consumer Code'] = bpl_sheet['Consumer No'].astype(str).str.replace('/', '', regex=False).str.strip()
        else:
            st.error("BPL data missing 'Consumer No' column.")
            return None
        return bpl_sheet[['Consumer Code', 'Present Status', 'Effect From', 'Effect To', 'Year', 'Month']].copy()
    except Exception as e:
        st.error(f"BPL processing failed: {e}")
        return None

def expand_bpl_years(bpl_df):
    expanded_rows = []
    bpl_df['Effect From'] = pd.to_datetime(bpl_df['Effect From'], errors='coerce')
    bpl_df['Effect To'] = pd.to_datetime(bpl_df['Effect To'], errors='coerce')
    for _, row in bpl_df.iterrows():
        start_year = row['Effect From'].year if pd.notna(row['Effect From']) else None
        end_year = row['Effect To'].year if pd.notna(row['Effect To']) else None
        if start_year is None or end_year is None:
            if pd.notna(row['Consumer Code']):
                expanded_rows.append({
                    'Consumer Code': str(row['Consumer Code']),
                    'Year': 'N/A',
                    'Present Status': row['Present Status'] if pd.notna(row['Present Status']) else 'Unknown'
                })
            continue
        try:
            for year in range(int(start_year), int(end_year) + 1):
                expanded_rows.append({
                    'Consumer Code': str(row['Consumer Code']),
                    'Year': year,
                    'Present Status': row['Present Status'] if pd.notna(row['Present Status']) else 'Unknown'
                })
        except Exception:
            if pd.notna(row['Consumer Code']):
                expanded_rows.append({
                    'Consumer Code': str(row['Consumer Code']),
                    'Year': 'Invalid Date Range',
                    'Present Status': row['Present Status'] if pd.notna(row['Present Status']) else 'Unknown'
                })
    return pd.DataFrame(expanded_rows)

# --------------------------- 
# Arear transformation (ported)
# --------------------------- 

def transform_arear_list_format(uploaded_file_bytes, sheet_name=None):
    """
    Attempt to transform a nonstandard Arear list into the standard format.
    Logic adapted from your original function.
    """
    # read with pandas skipping initial rows as original attempted
    try:
        # uploaded_file_bytes is BytesIO or file-like
        df = pd.read_excel(io.BytesIO(uploaded_file_bytes), sheet_name=sheet_name, skiprows=4, engine='openpyxl')
    except Exception:
        try:
            # try reading without skiprows
            df = pd.read_excel(io.BytesIO(uploaded_file_bytes), sheet_name=sheet_name, engine='openpyxl')
        except Exception as e:
            st.error(f"Failed to read Arear file for transformation: {e}")
            return None

    if df is None or df.empty:
        st.error("Arear file appears empty after read.")
        return None

    desired_header = [
        'Sl.No', 'Consumer No', 'area code', 'consumer code', 'category',
        'Primary key', 'Address', 'Phone', 'Route', 'Last Reading Date',
        'Last Pay Date', 'Last Amount Paid', 'Arrears', 'Disconn. Date'
    ]
    column_rename_map = {c: c for c in desired_header}  # not strict mapping here
    df.rename(columns=column_rename_map, inplace=True)

    if 'Consumer No' not in df.columns:
        st.error("Transformation Error: missing 'Consumer No' column.")
        return None

    # create area/consumer/category/key
    df[['area code', 'consumer code', 'category', 'Primary key']] = df['Consumer No'].apply(
        lambda x: pd.Series(_extract_consumer_info(x))
    )

    # create Arrears if not present
    if 'Arrears' not in df.columns:
        prev = df['PREVIOUS ARREAR'] if 'PREVIOUS ARREAR' in df.columns else 0
        curr = df['CURRENT ARREAR'] if 'CURRENT ARREAR' in df.columns else 0
        df['Arrears'] = pd.to_numeric(prev, errors='coerce').fillna(0) + pd.to_numeric(curr, errors='coerce').fillna(0)

    # add missing columns
    for col in desired_header:
        if col not in df.columns:
            df[col] = pd.NA

    # reorder and keep numeric Sl.No rows
    df_final = df[desired_header]
    df_final = df_final[df_final['Sl.No'].apply(lambda x: str(x).strip().isdigit() if pd.notna(x) else False)].copy()
    return df_final

# --------------------------- 
# Main analysis merge (ported)
# --------------------------- 

def merge_arear_reader_bpl(arear_df, reading_pdf_df, reader_df, bpl_df):
    """
    Merge dataframes according to original logic and compute thresholds/columns.
    reading_pdf_df is expected to have 'Primary key' and 'Previous Reading' columns.
    """
    try:
        arear_sheet = arear_df.dropna(how='all').copy()
        reading_sheet = reading_pdf_df.dropna(how='all').copy()
        reader_list = reader_df.dropna(how='all').copy()
        bpl_list_df = bpl_df.dropna(how='all').copy()

        # Expand BPL
        bpl_status_df_expanded_pivot = expand_bpl_years(bpl_list_df)
        if not bpl_status_df_expanded_pivot.empty:
            bpl_pivot = bpl_status_df_expanded_pivot.pivot_table(
                index='Consumer Code',
                columns='Year',
                values='Present Status',
                aggfunc='first'
            ).add_prefix('BPL_').reset_index()
        else:
            bpl_pivot = pd.DataFrame()

        # Ensure primary keys
        if 'Primary key' in arear_sheet.columns:
            arear_sheet['Primary key'] = arear_sheet['Primary key'].astype(str).str.strip()
        else:
            if 'Consumer No' in arear_sheet.columns:
                arear_sheet[['area code', 'consumer code', 'category', 'Primary key']] = arear_sheet['Consumer No'].apply(
                    lambda x: pd.Series(_extract_consumer_info(x))
                )
                arear_sheet['Primary key'] = arear_sheet['Primary key'].astype(str).str.strip()
            else:
                raise ValueError("Arear List missing 'Consumer No' and cannot create 'Primary key'.")

        if 'Primary key' in reading_sheet.columns:
            reading_sheet['Primary key'] = reading_sheet['Primary key'].astype(str).str.strip()

        # merge arear with reader by Route -> RouteCode
        merged_df = pd.merge(
            arear_sheet,
            reader_list[['RouteCode', 'Meter reader']],
            left_on='Route',
            right_on='RouteCode',
            how='left'
        )

        # merge with reading sheet
        merged_df = pd.merge(
            merged_df,
            reading_sheet[['Primary key', 'Previous Reading', 'Previous Reading Date', 'Consumer code']],
            left_on='Primary key',
            right_on='Primary key',
            how='left',
            suffixes=('_arear', '_pdf')
        )

        if 'Previous Reading' not in merged_df.columns:
            raise ValueError("'Previous Reading' column missing after merge.")

        merged_df.rename(columns={
            'Meter reader': 'Meter Reader Name',
            'Previous Reading': 'Final Reading',
            'Previous Reading Date': 'Last Reading Date PDF'
        }, inplace=True)

        # merge BPL
        if not bpl_pivot.empty:
            merged_df = pd.merge(
                merged_df,
                bpl_pivot,
                left_on='Primary key',
                right_on='Consumer Code',
                how='left'
            )

        # drop temporary columns
        merged_df.drop(columns=['RouteCode'], errors='ignore', inplace=True)

        # drop Primary key and redundant consumer code from arear list, keep the one from the PDF
        for col in ['Primary key', 'consumer code', 'Consumer Code_arear']:
            if col in merged_df.columns:
                merged_df.drop(columns=[col], inplace=True)
        
        if 'Consumer Code_pdf' in merged_df.columns:
             merged_df.rename(columns={'Consumer Code_pdf': 'Consumer code'}, inplace=True)


        # thresholds on Final Reading
        if 'Final Reading' in merged_df.columns:
            merged_df['Final Reading'] = pd.to_numeric(merged_df['Final Reading'], errors='coerce').fillna(0)
            for t in [100, 150, 200, 300, 500, 750, 1000]:
                merged_df[f'Reading > {t}'] = merged_df['Final Reading'] > t

        # Year/Month from Last Reading Date PDF
        if 'Last Reading Date PDF' in merged_df.columns:
            merged_df['Last Reading Date PDF'] = pd.to_datetime(merged_df['Last Reading Date PDF'], errors='coerce', dayfirst=True)
            merged_df['Year'] = merged_df['Last Reading Date PDF'].dt.year.fillna(-1).astype(int)
            merged_df['Month'] = merged_df['Last Reading Date PDF'].dt.month.fillna(-1).astype(int)
        else:
            merged_df['Year'] = -1
            merged_df['Month'] = -1

        return merged_df

    except Exception as e:
        st.error(f"Merge failed: {e}")
        st.exception(traceback.format_exc())
        return None

# ---------------------------
# Streamlit UI - Multi Tab Layout
# ---------------------------

st.set_page_config(page_title="Revenue Analysis - Streamlit", layout="wide")
st.title("Revenue Analysis ")

# Authentication & session state
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.role = None

if not st.session_state.logged_in:
    st.header("Login")
    st.write("Use one of the demo accounts: `user/userpass`")
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")
    if submitted:
        if username in USER_ROLES and USER_ROLES[username]["password"] == password:
            st.session_state.logged_in = True
            st.session_state.role = USER_ROLES[username]["role"]
            st.rerun()
        else:
            st.session_state.logged_in = False
            st.session_state.role = None
            st.error("Invalid username or password")
    st.stop()

st.sidebar.caption(f"Logged in as: **{st.session_state.role}**")
if st.sidebar.button("Logout"):
    st.session_state.logged_in = False
    st.session_state.role = None
    st.rerun()

# Tabs: File Uploads, Arear Analysis, BPL Analysis
tabs = st.tabs(["File Uploads", "Arear List Analysis", "BPL List Analysis"])

# ----- Tab 0: File Uploads -----
with tabs[0]:
    st.header("1. Upload files")

    st.subheader("Upload Arear List (Excel/CSV)")
    arear_template_bytes = build_template_bytes(REQUIRED_AREAR_COLUMNS_AREAR_LIST, sheet_name="ArearTemplate")
    st.download_button(
        label="Download Arear Template",
        data=arear_template_bytes,
        file_name="arear_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="download_arear_template"
    )
    arear_file = st.file_uploader("Arear List (Excel/CSV)", type=['xlsx', 'xls', 'csv'], key="arear")

    st.subheader("Upload BPL List (Excel/CSV)")
    bpl_template_bytes = build_template_bytes(REQUIRED_BPL_COLUMNS, sheet_name="BPLTemplate")
    st.download_button(
        label="Download BPL Template",
        data=bpl_template_bytes,
        file_name="bpl_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="download_bpl_template"
    )
    bpl_file = st.file_uploader("BPL List (Excel/CSV)", type=['xlsx', 'xls', 'csv'], key="bpl")

    st.subheader("Upload Reader List (Excel/CSV)")
    reader_template_bytes = build_template_bytes(REQUIRED_AREAR_COLUMNS_READER_LIST, sheet_name="ReaderTemplate")
    st.download_button(
        label="Download Reader Template",
        data=reader_template_bytes,
        file_name="reader_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="download_reader_template"
    )
    reader_file = st.file_uploader("Reader List (Excel/CSV)", type=['xlsx', 'xls', 'csv'], key="reader")

    st.subheader("Upload Reading Sheet PDFs (multiple allowed)")
    pdf_files = st.file_uploader("Reading Sheet PDFs", type=['pdf'], accept_multiple_files=True, key="pdfs")

    st.write("---")
    col1, col2 = st.columns(2)
    with col1:
        analyze_btn = st.button("Analyze & Merge All Arear Data", key="analyze")
    with col2:
        st.write("Status:")
        status_area = st.empty()

    # temporary holders in session_state
    if "uploaded_arear_df" not in st.session_state:
        st.session_state.uploaded_arear_df = None
    if "uploaded_bpl_df" not in st.session_state:
        st.session_state.uploaded_bpl_df = None
    if "uploaded_reader_df" not in st.session_state:
        st.session_state.uploaded_reader_df = None
    if "uploaded_pdf_df" not in st.session_state:
        st.session_state.uploaded_pdf_df = None
    if "merged_df" not in st.session_state:
        st.session_state.merged_df = None

    # Read each file when provided (but only when Analyze pressed we'll fully transform/merge)
    if arear_file is not None:
        raw_arear = read_excel_or_csv(arear_file)
        if isinstance(raw_arear, dict):
            # find a sheet with required columns
            sheet_df, sheet_name = find_sheet_by_columns(raw_arear, REQUIRED_AREAR_COLUMNS_AREAR_LIST)
            if sheet_df is not None:
                st.session_state.uploaded_arear_df = sheet_df.copy()
                st.info(f"Loaded Arear sheet: {sheet_name}")
            else:
                # store raw bytes for possible transformation
                st.session_state.uploaded_arear_df = ("NEEDS_TRANSFORM", arear_file.read())
                st.warning("Uploaded Arear file does not match standard format. We'll attempt transformation during analysis.")
        elif isinstance(raw_arear, pd.DataFrame):
            if check_columns_exist(raw_arear, REQUIRED_AREAR_COLUMNS_AREAR_LIST):
                st.session_state.uploaded_arear_df = raw_arear.copy()
                st.info("Loaded Arear CSV / single-sheet")
            else:
                st.session_state.uploaded_arear_df = ("NEEDS_TRANSFORM", arear_file.read())
                st.warning("Arear CSV does not match standard columns. We'll attempt transformation during analysis.")
        else:
            st.session_state.uploaded_arear_df = None

    if arear_file and "uploaded_arear_df" in st.session_state:
        arear_df_or_tuple = st.session_state.uploaded_arear_df
        if isinstance(arear_df_or_tuple, pd.DataFrame):
            st.write("Preview of loaded Arear data (first 5 rows):")
            st.dataframe(arear_df_or_tuple.head())

    if bpl_file is not None:
        raw_bpl = read_excel_or_csv(bpl_file)
        if isinstance(raw_bpl, dict):
            df, sname = find_sheet_by_columns(raw_bpl, REQUIRED_BPL_COLUMNS)
            if df is not None:
                processed = process_bpl_data_from_df(df)
                st.session_state.uploaded_bpl_df = processed
                st.success(f"BPL sheet loaded: {sname}")
            else:
                st.error("Uploaded BPL Excel does not contain required BPL columns.")
                st.session_state.uploaded_bpl_df = None
        elif isinstance(raw_bpl, pd.DataFrame):
            if check_columns_exist(raw_bpl, REQUIRED_BPL_COLUMNS):
                processed = process_bpl_data_from_df(raw_bpl)
                st.session_state.uploaded_bpl_df = processed
                st.success("BPL CSV loaded")
            else:
                st.error("Uploaded BPL CSV does not contain required columns.")
                st.session_state.uploaded_bpl_df = None

    if reader_file is not None:
        raw_reader = read_excel_or_csv(reader_file)
        if isinstance(raw_reader, dict):
            df, sname = find_sheet_by_columns(raw_reader, REQUIRED_AREAR_COLUMNS_READER_LIST)
            if df is not None:
                st.session_state.uploaded_reader_df = df.copy()
                st.success(f"Reader sheet loaded: {sname}")
            else:
                st.error("Uploaded Reader Excel does not contain required columns.")
                st.session_state.uploaded_reader_df = None
        elif isinstance(raw_reader, pd.DataFrame):
            if check_columns_exist(raw_reader, REQUIRED_AREAR_COLUMNS_READER_LIST):
                st.session_state.uploaded_reader_df = raw_reader.copy()
                st.success("Reader CSV loaded")
            else:
                st.error("Uploaded Reader CSV does not contain required columns.")
                st.session_state.uploaded_reader_df = None

    if pdf_files:
        # parse each PDF and concat
        all_dfs = []
        progress = st.progress(0)
        total = len(pdf_files)
        for idx, pdf in enumerate(pdf_files):
            try:
                bytes_data = pdf.read()
                route = extract_route_from_pdf_bytes(bytes_data)
                tab_df = pdfplumber_extract_tables(bytes_data)
                if tab_df is not None and not tab_df.empty:
                    norm = normalize_pdf_table(tab_df, route_no=route)
                    if norm is not None and not norm.empty:
                        all_dfs.append(norm)
                else:
                    st.warning(f"No tables extracted from {pdf.name}")
            except Exception as e:
                st.warning(f"Error processing {pdf.name}: {e}")
            progress.progress(int((idx + 1) / total * 100))
        progress.empty()
        if all_dfs:
            st.session_state.uploaded_pdf_df = pd.concat(all_dfs, ignore_index=True)
            st.success(f"Extracted and combined tables from {len(all_dfs)} PDF(s)")
        else:
            st.session_state.uploaded_pdf_df = None
            st.warning("No usable PDF data extracted.")

    if pdf_files and "uploaded_pdf_df" in st.session_state:
        pdf_df = st.session_state.uploaded_pdf_df
        if pdf_df is not None and not pdf_df.empty:
            st.write("Preview of extracted PDF data (first 5 rows):")
            st.dataframe(pdf_df.head())

    # Analyze / Merge when button pressed
    if analyze_btn:
        status_area.info("Starting analysis...")
        # validate presence
        # handle transformed arear if needed
        if st.session_state.uploaded_arear_df is None:
            st.error("Arear list not uploaded or not readable.")
        elif st.session_state.uploaded_reader_df is None:
            st.error("Reader list not uploaded or not readable.")
        elif st.session_state.uploaded_pdf_df is None:
            st.error("Reading Sheet PDFs not uploaded or no data extracted.")
        elif st.session_state.uploaded_bpl_df is None:
            st.error("BPL list not uploaded or not readable.")
        else:
            # If arear was flagged as needing transform, do it now
            if isinstance(st.session_state.uploaded_arear_df, tuple) and st.session_state.uploaded_arear_df[0] == "NEEDS_TRANSFORM":
                status_area.info("Attempting Arear transformation...")
                try:
                    bytes_blob = st.session_state.uploaded_arear_df[1]
                    transformed = transform_arear_list_format(bytes_blob, sheet_name=None)
                    if transformed is not None and not transformed.empty:
                        st.session_state.uploaded_arear_df = transformed
                        st.success("Arear file transformed successfully.")
                    else:
                        st.error("Transformation produced empty result.")
                except Exception as e:
                    st.error(f"Transformation failed: {e}")
                    st.session_state.uploaded_arear_df = None

            # now perform merge
            if isinstance(st.session_state.uploaded_arear_df, pd.DataFrame) and \
               isinstance(st.session_state.uploaded_reader_df, pd.DataFrame) and \
               isinstance(st.session_state.uploaded_pdf_df, pd.DataFrame) and \
               isinstance(st.session_state.uploaded_bpl_df, pd.DataFrame):
                status_area.info("Merging data...")
                merged = merge_arear_reader_bpl(
                    st.session_state.uploaded_arear_df,
                    st.session_state.uploaded_pdf_df,
                    st.session_state.uploaded_reader_df,
                    st.session_state.uploaded_bpl_df
                )
                if merged is not None:
                    st.session_state.merged_df = merged
                    status_area.success(f"Analysis complete. {merged.shape[0]} rows, {merged.shape[1]} columns.")
                else:
                    status_area.error("Merge returned no data.")
            else:
                status_area.error("One or more inputs are invalid after transformation. Check earlier messages.")

# ----- Tab 1: Arear List Analysis -----
with tabs[1]:
    st.header("Arear List Combined")
    if st.session_state.merged_df is None:
        st.info("No merged result available. Please upload files and run analysis in the 'File Uploads' tab.")
    else:
        df = st.session_state.merged_df.copy()
        st.write(f"Processed data: {df.shape[0]} rows, {df.shape[1]} columns")

        # Year / Month filters
        years = sorted([y for y in df['Year'].unique() if y != -1])
        months = sorted([m for m in df['Month'].unique() if m != -1])
        sel_years = st.multiselect("Filter Year(s)", years, default=years)
        sel_month = st.selectbox("Filter Month", ["All"] + months, index=0)

        display_df = df.copy()
        if sel_years:
            display_df = display_df[display_df['Year'].isin(sel_years)]
        if sel_month != "All":
            display_df = display_df[display_df['Month'] == int(sel_month)]

        st.dataframe(display_df, use_container_width=True)

        # Download button for merged excel
        towrite = io.BytesIO()
        downloaded_filename = f"merged_arear_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with pd.ExcelWriter(towrite, engine="openpyxl") as writer:
            display_df.to_excel(writer, index=False, sheet_name="MergedArear")
        towrite.seek(0)
        st.download_button(label="Download Merged Arear Excel", data=towrite, file_name=downloaded_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ----- Tab 2: BPL List Analysis -----
with tabs[2]:
    st.header("BPL List Analysis (Standalone)")
    st.write("You can re-upload a BPL file here for standalone analysis (or just use the one uploaded in File Uploads).")
    standalone_bpl = st.file_uploader("Upload BPL (optional, Excel/CSV)", type=['xlsx', 'xls', 'csv'], key="bpl_standalone")
    if standalone_bpl is not None:
        raw = read_excel_or_csv(standalone_bpl)
        if isinstance(raw, dict):
            df, sname = find_sheet_by_columns(raw, REQUIRED_BPL_COLUMNS)
            if df is not None:
                processed = process_bpl_data_from_df(df)
                st.session_state.bpl_standalone_df = processed
                st.success(f"BPL sheet loaded: {sname}")
            else:
                st.error("Uploaded BPL does not have required columns.")
        elif isinstance(raw, pd.DataFrame):
            if check_columns_exist(raw, REQUIRED_BPL_COLUMNS):
                processed = process_bpl_data_from_df(raw)
                st.session_state.bpl_standalone_df = processed
                st.success("BPL CSV loaded")
            else:
                st.error("Uploaded BPL CSV missing columns.")
    else:
        # fallback to previously uploaded
        if st.session_state.uploaded_bpl_df is not None:
            st.session_state.bpl_standalone_df = st.session_state.uploaded_bpl_df

    if "bpl_standalone_df" in st.session_state and st.session_state.bpl_standalone_df is not None:
        bpldf = st.session_state.bpl_standalone_df.copy()
        st.write(f"BPL processed rows: {bpldf.shape[0]}")
        st.dataframe(bpldf, use_container_width=True)
        # summary
        st.write("Present Status counts:")
        st.write(bpldf['Present Status'].value_counts(dropna=False))
        # download
        bio = io.BytesIO()
        fname = f"bpl_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            bpldf.to_excel(writer, index=False, sheet_name="BPL")
        bio.seek(0)
        st.download_button("Download BPL Processed Excel", data=bio, file_name=fname, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("No BPL data processed yet.")
