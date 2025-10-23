# app.py
import io
import base64
import re
import traceback
from datetime import datetime
from functools import lru_cache
from pathlib import Path
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
        uploaded_file.seek(0)
    except Exception:
        pass
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
                        # broaden the window slightly to catch consumers split across lines
                        window = " ".join(lines[i:i + 3])
                        cons_match = consumer_pattern.search(window)
                        if not cons_match:
                            continue

                    # Found a consumer identifier; gather surrounding context to extract date/readings
                    area, code, cat = cons_match.groups()
                    consumer_no = cons_match.group(0)
                    primary_key = f"{area}{code}{cat}"

                    # Build a context window spanning a couple lines before and after the match
                    start = max(0, i - 2)
                    end = min(len(lines), i + 3)
                    context = " ".join(lines[start:end])

                    # Try to find a labeled previous reading first, then fall back to heuristics
                    prev_reading = ""
                    m_labeled = labeled_reading_pattern.search(context)
                    if m_labeled:
                        prev_reading = m_labeled.group(1)
                    else:
                        # Look for patterns like "Prev 12345" or "Previous 12345" near the consumer block
                        m_near = re.search(
                            r"(?:Prev(?:ious)?|Last)[^\d\-]{0,10}(-?\d{1,7}(?:\.\d+)?)",
                            context,
                            re.IGNORECASE,
                        )
                        if m_near:
                            prev_reading = m_near.group(1)
                        else:
                            # Fallback: pick the first reasonable numeric token that doesn't look like a date
                            for num in number_pattern.findall(context):
                                # skip tokens that are likely parts of dates (e.g., day or year fragments)
                                if date_pattern.search(num):
                                    continue
                                # skip tokens that include separators like '/' which indicate non-reading tokens
                                if "/" in num or "-" in num and len(num) > 0 and not re.match(r"-?\d{1,7}(?:\.\d+)?$", num):
                                    continue
                                # ensure the numeric length is reasonable (up to 7 digits before decimal)
                                if re.match(r"^-?\d{1,7}(?:\.\d+)?$", num):
                                    prev_reading = num
                                    break

                    # Try to find an associated date in the same context
                    prev_date = ""
                    m_date = date_pattern.search(context)
                    if m_date:
                        prev_date = m_date.group(0)

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
        r"(Prev(?:ious)?|Last)\s*(Read(?:ing)?|Rdg)(?!\s*(Date|Dt\. ?))": "Previous Reading",
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

        # merge with reading sheet - keep all arear rows
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

        # Build priority list for consumer code preservation:
        # prefer arear consumer code variants first, then existing consumer code, then PDF variants
        consumer_code_priority = [
            col for col in [
                'Consumer code_arear',
                'Consumer Code_arear',
                'consumer code',
                'Consumer Code',
                'Consumer code_pdf',
                'Consumer Code_pdf'
            ]
            if col in merged_df.columns
        ]
        if consumer_code_priority:
            merged_df['Consumer code'] = merged_df[consumer_code_priority].apply(
                lambda row: next(
                    (str(val).strip() for val in row if pd.notna(val) and str(val).strip()),
                    None
                ),
                axis=1
            )

        # merge BPL
        if not bpl_pivot.empty:
            merged_df = pd.merge(
                merged_df,
                bpl_pivot,
                left_on='Primary key',
                right_on='Consumer Code',
                how='left'
            )

        # thresholds on Final Reading
        if 'Final Reading' in merged_df.columns:
            merged_df['Final Reading'] = pd.to_numeric(merged_df['Final Reading'], errors='coerce')
            # Treat missing or non-numeric Final Reading as 0 for threshold checks
            merged_df['Final Reading'].fillna(0, inplace=True)
            for t in [100, 150, 200, 300, 500, 750, 1000]:
                merged_df[f'Reading > {t}'] = merged_df['Final Reading'] > t
        else:
            # ensure the column exists and threshold flags are present
            merged_df['Final Reading'] = 0
            for t in [100, 150, 200, 300, 500, 750, 1000]:
                merged_df[f'Reading > {t}'] = False

        # Year/Month from Last Reading Date PDF
        if 'Last Reading Date PDF' in merged_df.columns:
            merged_df['Last Reading Date PDF'] = pd.to_datetime(
                merged_df['Last Reading Date PDF'], errors='coerce', dayfirst=True
            )
            merged_df['Year'] = merged_df['Last Reading Date PDF'].dt.year.fillna(-1).astype(int)
            merged_df['Month'] = merged_df['Last Reading Date PDF'].dt.month.fillna(-1).astype(int)
        else:
            merged_df['Last Reading Date PDF'] = pd.NaT
            merged_df['Year'] = -1
            merged_df['Month'] = -1

        # drop temporary columns
        merged_df.drop(columns=['RouteCode'], errors='ignore', inplace=True)

        # drop extra consumer code columns but keep the consolidated 'Consumer code'
        extra_consumer_cols = [
            col for col in [
                'consumer code',
                'Consumer Code_arear',
                'Consumer code_arear',
                'Consumer Code_pdf',
                'Consumer code_pdf',
                'Consumer Code'
            ]
            if col in merged_df.columns and col != 'Consumer code'
        ]
        merged_df.drop(columns=extra_consumer_cols, errors='ignore', inplace=True)

        # finally drop Primary key if present
        if 'Primary key' in merged_df.columns:
            merged_df.drop(columns=['Primary key'], inplace=True)

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

# Tabs: File Uploads, Arear Analysis
tabs = st.tabs(["File Uploads", "Arear List Analysis"])

# ----- Tab 0: File Uploads ----- 
with tabs[0]:
    st.header("1. Upload Files")

    # Initialize session state for file uploads if they don't exist
    if "arear_file" not in st.session_state:
        st.session_state.arear_file = None
    if "bpl_file" not in st.session_state:
        st.session_state.bpl_file = None
    if "reader_file" not in st.session_state:
        st.session_state.reader_file = None
    if "pdf_files" not in st.session_state:
        st.session_state.pdf_files = None
    if "merged_df" not in st.session_state:
        st.session_state.merged_df = None

    # --- File Upload Section ---
    col1, col2 = st.columns([3, 1])
    with col1:
        st.session_state.arear_file = st.file_uploader("Upload Arrear List (Excel/CSV)", type=['xlsx', 'xls', 'csv'], key="arear")
        
    # --- Template Download ---
    template_bytes = build_template_bytes(REQUIRED_AREAR_COLUMNS_AREAR_LIST)
    st.download_button(
        label="Download Arrear Template",
        data=template_bytes,
        file_name="arrear_template.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    with col2:
        if st.session_state.arear_file:
            st.markdown('<div style="background-color: #28a745; color: white; padding: 10px; border-radius: 5px; text-align: center;"><b>Uploaded ✅</b></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="background-color: #ffc107; color: black; padding: 10px; border-radius: 5px; text-align: center;"><i>Waiting...</i></div>', unsafe_allow_html=True)

    col3, col4 = st.columns([3, 1])
    with col3:
        st.session_state.bpl_file = st.file_uploader("Upload BPL List (Excel/CSV)", type=['xlsx', 'xls', 'csv'], key="bpl")
        # --- Template Download ---
        template_bytes = build_template_bytes(REQUIRED_BPL_COLUMNS)
        st.download_button(
            label="Download BPL Template",
            data=template_bytes,
            file_name="bpl_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    with col4:
        if st.session_state.bpl_file:
            st.markdown('<div style="background-color: #28a745; color: white; padding: 10px; border-radius: 5px; text-align: center;"><b>Uploaded ✅</b></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="background-color: #ffc107; color: black; padding: 10px; border-radius: 5px; text-align: center;"><i>Waiting...</i></div>', unsafe_allow_html=True)

    col5, col6 = st.columns([3, 1])
    with col5:
        st.session_state.reader_file = st.file_uploader("Upload Reader List (Excel/CSV)", type=['xlsx', 'xls', 'csv'], key="reader")
        # --- Template Download ---
        template_bytes = build_template_bytes(REQUIRED_AREAR_COLUMNS_READER_LIST)
        st.download_button(
            label="Download Reader Template",
            data=template_bytes,
            file_name="reader_template.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    with col6:
        if st.session_state.reader_file:
            st.markdown('<div style="background-color: #28a745; color: white; padding: 10px; border-radius: 5px; text-align: center;"><b>Uploaded ✅</b></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="background-color: #ffc107; color: black; padding: 10px; border-radius: 5px; text-align: center;"><i>Waiting...</i></div>', unsafe_allow_html=True)

    col7, col8 = st.columns([3, 1])
    with col7:
        st.session_state.pdf_files = st.file_uploader("Upload Reading Sheets (PDF)", type=['pdf'], accept_multiple_files=True, key="pdfs")
    with col8:
        if st.session_state.pdf_files:
            st.markdown('<div style="background-color: #28a745; color: white; padding: 10px; border-radius: 5px; text-align: center;"><b>Uploaded ✅</b></div>', unsafe_allow_html=True)
        else:
            st.markdown('<div style="background-color: #ffc107; color: black; padding: 10px; border-radius: 5px; text-align: center;"><i>Waiting...</i></div>', unsafe_allow_html=True)

    st.write("---")

    # --- Progress and Analysis Section ---
    uploaded_files_count = sum([
        1 for f in [st.session_state.arear_file, st.session_state.bpl_file, st.session_state.reader_file, st.session_state.pdf_files] if f
    ])
    progress_percentage = int((uploaded_files_count / 4) * 80)

    st.header("Analysis Progress")
    progress_bar = st.progress(progress_percentage)

    all_files_uploaded = uploaded_files_count == 4
    
    analyze_btn = st.button(
        "Analyze & Merge All Area Data",
        key="analyze",
        use_container_width=True,
        disabled=not all_files_uploaded
    )

    if analyze_btn:
        progress_bar.progress(85)
        st.info("Processing files...")
        
        # Add a separate status bar for reading sheet processing
        st.subheader("Reading Sheet Processing")
        reading_sheet_progress = st.progress(0)
        reading_status = st.empty()  # Create a placeholder for the status text
        
        # --- Data Processing Logic (moved here) ---
        
        # 1. Process Arear File
        arear_df = None
        if st.session_state.arear_file:
            raw_arear = read_excel_or_csv(st.session_state.arear_file)
            if isinstance(raw_arear, dict):
                sheet_df, sheet_name = find_sheet_by_columns(raw_arear, REQUIRED_AREAR_COLUMNS_AREAR_LIST)
                if sheet_df is not None:
                    arear_df = sheet_df.copy()
                    st.info(f"Loaded Arear sheet: {sheet_name}")
                else:
                    try:
                        st.session_state.arear_file.seek(0)
                        arear_bytes = st.session_state.arear_file.read()
                        arear_df = transform_arear_list_format(arear_bytes)
                        if arear_df is not None and not arear_df.empty:
                             st.info("Arear file transformed successfully.")
                        else:
                             st.error("Arear file transformation failed.")
                    except Exception as e:
                        st.error(f"Arear file transformation failed: {e}")

            elif isinstance(raw_arear, pd.DataFrame):
                if check_columns_exist(raw_arear, REQUIRED_AREAR_COLUMNS_AREAR_LIST):
                    arear_df = raw_arear.copy()
                    st.info("Loaded Arear CSV / single-sheet")
                else:
                    st.error("Arear CSV does not match standard columns.")
        
        # 2. Process BPL File
        bpl_df = None
        if st.session_state.bpl_file:
            raw_bpl = read_excel_or_csv(st.session_state.bpl_file)
            if isinstance(raw_bpl, dict):
                df, sname = find_sheet_by_columns(raw_bpl, REQUIRED_BPL_COLUMNS)
                if df is not None:
                    bpl_df = process_bpl_data_from_df(df)
                    if bpl_df is not None:
                        st.success(f"BPL sheet loaded: {sname}")
            elif isinstance(raw_bpl, pd.DataFrame):
                 if check_columns_exist(raw_bpl, REQUIRED_BPL_COLUMNS):
                    bpl_df = process_bpl_data_from_df(raw_bpl)
                    if bpl_df is not None:
                        st.success("BPL CSV loaded")

        # 3. Process Reader File
        reader_df = None
        if st.session_state.reader_file:
            raw_reader = read_excel_or_csv(st.session_state.reader_file)
            if isinstance(raw_reader, dict):
                df, sname = find_sheet_by_columns(raw_reader, REQUIRED_AREAR_COLUMNS_READER_LIST)
                if df is not None:
                    reader_df = df.copy()
                    st.success(f"Reader sheet loaded: {sname}")
            elif isinstance(raw_reader, pd.DataFrame):
                if check_columns_exist(raw_reader, REQUIRED_AREAR_COLUMNS_READER_LIST):
                    reader_df = raw_reader.copy()
                    st.success("Reader CSV loaded")

        # 4. Process PDF Files
        pdf_df = None
        if st.session_state.pdf_files:
            all_dfs = []
            total_pdfs = len(st.session_state.pdf_files)
            
            for idx, pdf in enumerate(st.session_state.pdf_files):
                try:
                    # Update reading sheet progress bar and status text
                    reading_sheet_progress.progress((idx / total_pdfs))
                    reading_status.text(f"Processing reading sheet {idx+1} out of {total_pdfs}")
                    
                    bytes_data = pdf.read()
                    route = extract_route_from_pdf_bytes(bytes_data)
                    tab_df = pdfplumber_extract_tables(bytes_data)
                    if tab_df is not None and not tab_df.empty:
                        norm = normalize_pdf_table(tab_df, route_no=route)
                        if norm is not None and not norm.empty:
                            all_dfs.append(norm)
                except Exception as e:
                    st.warning(f"Error processing {pdf.name}: {e}")
            if all_dfs:
                pdf_df = pd.concat(all_dfs, ignore_index=True)
                st.success(f"Extracted and combined tables from {len(all_dfs)} PDF(s)")

        # 5. Merge Data
        if all([arear_df is not None, bpl_df is not None, reader_df is not None, pdf_df is not None]):
            st.session_state.merged_df = merge_arear_reader_bpl(
                arear_df,
                pdf_df,
                reader_df,
                bpl_df
            )
            if st.session_state.merged_df is not None:
                st.success(f"Analysis complete. {st.session_state.merged_df.shape[0]} rows, {st.session_state.merged_df.shape[1]} columns.")
                st.info("Navigate to the 'Arear List Analysis' tab to view the results.")
            else:
                st.error("Merge returned no data. Check the input files and formats.")
        else:
            st.error("One or more files could not be processed. Please check the files and try again.")

# ----- Tab 1: Arear List Analysis ----- 
with tabs[1]:
    st.header("Arear List Combined")
    if st.session_state.merged_df is None:
        st.info("No merged result available. Please upload files and run analysis in the 'File Uploads' tab.")
    else:
        df = st.session_state.merged_df.copy()
        st.write(f"Processed data: {df.shape[0]} rows, {df.shape[1]} columns")

        # Pivot table for consumer count per area and category
        st.subheader("Consumer Count per Area and Category")
        show_live_pivot = st.checkbox("Filter pivot table for live connections only")

        pivot_df = df.copy()
        if show_live_pivot:
            pivot_df = pivot_df[pd.isna(pivot_df['Disconn. Date']) | (pivot_df['Disconn. Date'] == '')]

        pivot_table = pd.pivot_table(
            pivot_df,
            index='area code',
            columns='category',
            values='Consumer No',
            aggfunc='count',
            fill_value=0
        )
        st.write(pivot_table)

        st.dataframe(df, use_container_width=True)

        # Download button for merged excel
        towrite = io.BytesIO()
        downloaded_filename = f"merged_arear_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with pd.ExcelWriter(towrite, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="MergedArear")
        towrite.seek(0)
        st.download_button(label="Download Merged Arear Excel", data=towrite, file_name=downloaded_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# BPL List Analysis tab removed