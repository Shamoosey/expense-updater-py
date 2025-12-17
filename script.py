import os
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import configparser
import logging
from logging.handlers import RotatingFileHandler

# Load configuration
config = configparser.ConfigParser()
config_file = 'config.ini'

if not os.path.exists(config_file):
    print(f"ERROR: Configuration file '{config_file}' not found!")
    sys.exit(1)

config.read(config_file)

# Get configuration values
UPLOADS_PATH = config.get('Paths', 'uploads_path')
SERVICE_ACCOUNT_FILE = config.get('Paths', 'service_account_file')
LOG_FOLDER = config.get('Paths', 'log_folder')

# Create logs folder if it doesn't exist
if not os.path.exists(LOG_FOLDER):
    os.makedirs(LOG_FOLDER)

# Generate log filename with current date and time
log_filename = datetime.now().strftime('%Y-%m-%d_%H-%M-%S_budget_sync.log')
LOG_FILE = os.path.join(LOG_FOLDER, log_filename)

GOOGLE_SHEET_NAME = config.get('GoogleSheets', 'sheet_name')
EXPENSE_SHEET = config.get('GoogleSheets', 'expense_sheet')
INCOME_SHEET = config.get('GoogleSheets', 'income_sheet')

CSV_COLS = config.get('CSV', 'columns').split(',')

DATE_FORMAT = config.get('DataProcessing', 'date_format')
FILTER_STRINGS = [s.strip() for s in config.get('DataProcessing', 'filter_strings').split(',')]
KEEP_ACCOUNT_NUMBERS = [s.strip() for s in config.get('DataProcessing', 'keep_account_numbers').split(',')]

DATA_START_ROW = config.getint('Sheets', 'data_start_row')
DATA_START_COLUMN = config.get('Sheets', 'data_start_column')
DATA_END_COLUMN = config.get('Sheets', 'data_end_column')
AMOUNT_COLUMN = config.get('Sheets', 'amount_column')

LOG_LEVEL = config.get('Logging', 'log_level')
LOG_FORMAT = config.get('Logging', 'log_format')
LOG_DATE_FORMAT = config.get('Logging', 'log_date_format')
MAX_LOG_SIZE = config.getint('Logging', 'max_log_size')
BACKUP_COUNT = config.getint('Logging', 'backup_count')
CONSOLE_LOGGING = config.getboolean('Logging', 'console_logging')

# Setup logging
logger = logging.getLogger('BudgetSync')
logger.setLevel(getattr(logging, LOG_LEVEL))

# Create formatter with UTF-8 encoding support
formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

# File handler with rotation and UTF-8 encoding
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=MAX_LOG_SIZE,
    backupCount=BACKUP_COUNT,
    encoding='utf-8'
)
file_handler.setLevel(getattr(logging, LOG_LEVEL))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Console handler with UTF-8 encoding (optional)
if CONSOLE_LOGGING:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, LOG_LEVEL))
    console_handler.setFormatter(formatter)
    # Set console encoding to UTF-8
    if hasattr(console_handler.stream, 'reconfigure'):
        console_handler.stream.reconfigure(encoding='utf-8')
    logger.addHandler(console_handler)

# Pandas display settings
pd.set_option('display.max_rows', None)

logger.info("="*60)
logger.info("Budget Sync Started")
logger.info("="*60)
logger.info(f"Log file: {LOG_FILE}")

try:
    # Auth setup
    logger.info("Authenticating with Google Sheets API...")
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=scope
    )
    client = gspread.authorize(creds)

    logger.info("Authentication successful")

    # Open Google Sheets
    logger.info(f"Opening Google Sheet: '{GOOGLE_SHEET_NAME}'...")
    expense_sheet = client.open(GOOGLE_SHEET_NAME).worksheet(EXPENSE_SHEET)
    income_sheet = client.open(GOOGLE_SHEET_NAME).worksheet(INCOME_SHEET)
    logger.info(f"Successfully opened sheets: '{EXPENSE_SHEET}' and '{INCOME_SHEET}'")

    # Verify uploads directory
    if not os.path.isdir(UPLOADS_PATH):
        logger.error(f"Uploads folder not found: {UPLOADS_PATH}")
        raise FileNotFoundError(f"Uploads folder not found: {UPLOADS_PATH}")
    
    logger.info(f"Uploads directory found: {UPLOADS_PATH}")

    # Load CSV files
    logger.info("Loading CSV files...")
    dfs = []
    csv_files = [f for f in os.listdir(UPLOADS_PATH) if f.lower().endswith(".csv")]
    
    if not csv_files:
        logger.error("No CSV files found in uploads directory")
        raise ValueError("No CSV files found in uploads directory.")
    
    logger.info(f"Found {len(csv_files)} CSV file(s)")
    
    for filename in csv_files:
        file_path = os.path.join(UPLOADS_PATH, filename)
        logger.info(f"Loading: {filename}")
        df = pd.read_csv(file_path, sep=",", names=CSV_COLS)
        dfs.append(df)
        logger.debug(f"Loaded {len(df)} rows from {filename}")

    full_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Combined data: {len(full_df)} total rows")

    # Format all datetimes
    logger.info("Formatting dates...")
    full_df['date'] = pd.to_datetime(full_df['date'], format='mixed')
    full_df = full_df.sort_values(by="date", ascending=True).reset_index(drop=True)
    full_df['date'] = full_df['date'].dt.strftime(DATE_FORMAT)
    logger.debug(f"Date range: {full_df['date'].min()} to {full_df['date'].max()}")

    # Filter out unwanted transactions
    logger.info("Filtering transactions...")
    initial_count = len(full_df)
    
    # Build filter condition
    filter_condition = pd.Series([True] * len(full_df))
    
    for filter_str in FILTER_STRINGS:
        # Check if transaction name contains any filter string
        contains_filter = full_df['name'].str.contains(filter_str, case=False, na=False)
        
        # Check if transaction name contains any keep account number
        contains_keep_account = pd.Series([False] * len(full_df))
        for keep_account in KEEP_ACCOUNT_NUMBERS:
            contains_keep_account |= full_df['name'].str.contains(keep_account, case=False, na=False)
        
        # Filter out if contains filter string AND does not contain keep account
        filter_condition &= ~(contains_filter & ~contains_keep_account)
    
    full_df = full_df[filter_condition]
    filtered_count = initial_count - len(full_df)
    logger.info(f"Filtered out {filtered_count} transactions")

    payments = full_df[full_df['payment'].notna() & (full_df['payment'] > 0)]
    debts = full_df[full_df['cost'].notna() & (full_df['cost'] > 0)]
    
    logger.info(f"Found {len(payments)} payment(s) and {len(debts)} expense(s)")

    # Get existing data from sheets
    data_range = f'{DATA_START_COLUMN}{DATA_START_ROW}:{DATA_END_COLUMN}'
    logger.info(f"Fetching existing data from range: {data_range}")
    
    expense_data_rows = expense_sheet.get(data_range)
    income_data_rows = income_sheet.get(data_range)
    
    logger.info(f"Existing data: {len(expense_data_rows)} expense rows, {len(income_data_rows)} income rows")

    def row_exists(date_str, name_str, amount_val, sheet_data, csv_data):
      """Check if a row with matching name and amount exists in sheet or CSV data"""
      name_str_clean = name_str.strip().lower()

      # --- Check Google Sheet data ---
      for row in sheet_data:
          if len(row) >= 3:
              existing_name = row[1].strip().lower() if row[1] else ""

              if existing_name != name_str_clean:
                  continue

              try:
                  existing_amount = float(
                      str(row[2]).replace('$', '').replace(',', '').strip()
                  )
              except (ValueError, TypeError):
                  continue

              if abs(existing_amount - amount_val) <= 0.01:
                  return True

      # --- Check CSV data (exclude self) ---
      for csv_row in csv_data.itertuples():
          csv_name = csv_row.name.strip().lower()

          if csv_name != name_str_clean:
              continue

          csv_amount = None
          if hasattr(csv_row, 'cost') and pd.notna(csv_row.cost) and csv_row.cost > 0:
              csv_amount = float(csv_row.cost)
          elif hasattr(csv_row, 'payment') and pd.notna(csv_row.payment) and csv_row.payment > 0:
              csv_amount = float(csv_row.payment)

          if csv_amount is None:
              continue

          # Skip matching the same row
          if abs(csv_amount - amount_val) <= 0.01 and csv_row.date != date_str:
              return True

      return False

    def merge_and_sort_data(existing_data, new_rows):
        """Merge existing data with new rows and sort by date"""
        all_rows = []
        
        # Add existing rows
        for row in existing_data:
            if len(row) >= 1 and row[0]:  # Has a date
                all_rows.append({
                    'date': row[0].strip(),
                    'name': row[1].strip() if len(row) > 1 else '',
                    'amount': row[2] if len(row) > 2 else '',
                    'category': row[3] if len(row) > 3 else '',
                    'notes': row[4] if len(row) > 4 else '',
                    'date_obj': None
                })
        
        # Add new rows
        for new_row in new_rows:
            all_rows.append(new_row)
        
        # Parse dates and sort
        for row in all_rows:
            try:
                row['date_obj'] = datetime.strptime(row['date'], DATE_FORMAT)
            except ValueError:
                row['date_obj'] = datetime.min
                logger.warning(f"Invalid date format: {row['date']}")
        
        all_rows.sort(key=lambda x: x['date_obj'])
        
        # Convert back to list format for sheets
        result = []
        for row in all_rows:
            result.append([
                row['date'],
                row['name'],
                row['amount'],
                row['category'],
                row['notes']
            ])
        
        return result

    # Process expenses (debts)
    logger.info("="*60)
    logger.info("Processing Expenses")
    logger.info("="*60)
    new_expense_rows = []

    for index, row in debts.iterrows():
        date_val = row['date']
        name_val = row['name']
        cost_val = float(row['cost'])
        
        if not row_exists(date_val, name_val, cost_val, expense_data_rows, full_df):
            new_expense_rows.append({
                'date': date_val,
                'name': name_val,
                'amount': cost_val,
                'category': '',
                'notes': '',
                'date_obj': None
            })
            logger.info(f"Queued expense: {date_val} - {name_val} - ${cost_val:.2f}")
        else:
            logger.info(f"Skipping duplicate expense (same name/amount within 3 days): {date_val} - {name_val} - ${cost_val:.2f}")

    if new_expense_rows:
        logger.info(f"Merging {len(new_expense_rows)} new expense row(s) with existing data...")
        merged_expense_data = merge_and_sort_data(expense_data_rows, new_expense_rows)
        
        logger.info(f"Updating expense sheet with {len(merged_expense_data)} total rows...")
        expense_sheet.batch_clear([data_range])
        
        if merged_expense_data:
            expense_sheet.update(range_name=data_range, values=merged_expense_data, value_input_option='USER_ENTERED')
            
            # Format amount column as currency
            num_rows = len(merged_expense_data)
            format_range = f'{AMOUNT_COLUMN}{DATA_START_ROW}:{AMOUNT_COLUMN}{DATA_START_ROW + num_rows - 1}'
            expense_sheet.format(format_range, {
                "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}
            })
            logger.debug(f"Applied currency formatting to range: {format_range}")
        
        logger.info(f"[OK] Added {len(new_expense_rows)} new expense row(s)")
    else:
        logger.info("No new expense rows to add")

    # Process income (payments)
    logger.info("="*60)
    logger.info("Processing Income")
    logger.info("="*60)
    new_income_rows = []

    for index, row in payments.iterrows():
        date_val = row['date']
        name_val = row['name']
        payment_val = float(row['payment'])
        
        if not row_exists(date_val, name_val, payment_val, income_data_rows, full_df):
            new_income_rows.append({
                'date': date_val,
                'name': name_val,
                'amount': payment_val,
                'category': '',
                'notes': '',
                'date_obj': None
            })
            logger.info(f"Queued income: {date_val} - {name_val} - ${payment_val:.2f}")
        else:
            logger.info(f"Skipping duplicate income (same name/amount within 3 days): {date_val} - {name_val} - ${payment_val:.2f}")

    if new_income_rows:
        logger.info(f"Merging {len(new_income_rows)} new income row(s) with existing data...")
        merged_income_data = merge_and_sort_data(income_data_rows, new_income_rows)
        
        logger.info(f"Updating income sheet with {len(merged_income_data)} total rows...")
        income_sheet.batch_clear([data_range])
        
        if merged_income_data:
            income_sheet.update(range_name=data_range, values=merged_income_data, value_input_option='USER_ENTERED')
            
            # Format amount column as currency
            num_rows = len(merged_income_data)
            format_range = f'{AMOUNT_COLUMN}{DATA_START_ROW}:{AMOUNT_COLUMN}{DATA_START_ROW + num_rows - 1}'
            income_sheet.format(format_range, {
                "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}
            })
            logger.debug(f"Applied currency formatting to range: {format_range}")
        
        logger.info(f"[OK] Added {len(new_income_rows)} new income row(s)")
    else:
        logger.info("No new income rows to add")

    logger.info("="*60)
    logger.info("Update Complete - Success!")
    logger.info("="*60)

except FileNotFoundError as e:
    logger.error(f"File not found: {e}")
    sys.exit(1)
except ValueError as e:
    logger.error(f"Value error: {e}")
    sys.exit(1)
except gspread.exceptions.SpreadsheetNotFound:
    logger.error(f"Google Sheet '{GOOGLE_SHEET_NAME}' not found")
    sys.exit(1)
except gspread.exceptions.WorksheetNotFound as e:
    logger.error(f"Worksheet not found: {e}")
    sys.exit(1)
except Exception as e:
    logger.exception(f"Unexpected error occurred: {e}")
    sys.exit(1)