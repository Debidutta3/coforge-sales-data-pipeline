from src.database import load_sales_to_database #import the function to load the data into the database
from pathlib import Path #construct the file path
import pandas as pd

import logging #to check the logs and debug the code

#current python file path (__file__)
#resolve() returns the absolute path of the file
#parent.parent returns the parent directory of the parent directory (i.e., the project root)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

#where is the actual data
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw_sales.csv"

#data path for processed and rejected data
PROCESSED_DATA_PATH = PROJECT_ROOT / "data" / "processed_sales.csv"
REJECTED_DATA_PATH = PROJECT_ROOT / "data" / "rejected_sales.csv"

#LOGS
#save the logs in a logs directory under the project root
LOG_FILE = PROJECT_ROOT / "logs" / "pipeline.log"

logging.basicConfig(
    level=logging.INFO, #saves the information
    format="%(asctime)s | %(levelname)s | %(message)s", #adds timestamps to the logs
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"), #writes messages to a log file
        logging.StreamHandler(), #displays the message on terminal
    ],
)

logger = logging.getLogger(__name__)

#structure of the schema
EXPECTED_COLUMNS = [
    "order_id",
    "order_date",
    "product",
    "quantity",
    "unit_price",
    "customer_id",
]

#columns that should be numeric (numbers)
NUMERIC_COLUMNS = [
    "order_id",
    "quantity",
    "unit_price",
    "customer_id",
]

#expects a file path and returns a pandas DataFrame
def ingest_sales_data(file_path: Path) -> pd.DataFrame:
    """Read sales data and verify the required columns."""
    
    #if the file does not exist, raise a FileNotFoundError
    if not file_path.is_file():
        raise FileNotFoundError(f"Sales file not found: {file_path}")

    df = pd.read_csv(file_path)

    #check for missing required columns
    missing_columns = [
        column for column in EXPECTED_COLUMNS
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    #return copy as we dont want to modify the original DataFrame
    return df[EXPECTED_COLUMNS].copy()


#validation and transformation function
#it accepts one dataframe and returns a tuple of two dataframes (valid and rejected)
def validate_and_transform(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separate invalid records and transform valid sales records."""
    
    df = df.copy() #dont modify the original DataFrame

    # Normalize text fields and identify missing required values.
    for column in ["order_date", "product"]:
        
        #convert the column to string type and strip whitespace from both ends  
        df[column] = df[column].astype("string").str.strip()

    reasons = pd.Series("", index=df.index, dtype="string")

    def add_reason(mask: pd.Series, message: str) -> None:
        """Append a rejection reason to each matching record."""
        nonlocal reasons
        reasons.loc[mask] = reasons.loc[mask].apply(
            lambda existing: (
                f"{existing}; {message}" if existing else message
            )
        )

    # Check required missing fields .
    for column in EXPECTED_COLUMNS:
        missing = df[column].isna() #isna identifies missing values (NaN) in the column

        if column == "product":
            missing = missing | df[column].eq("")

        add_reason(missing, f"Missing required value: {column}")

    # Convert numeric fields safely; invalid text becomes NaN.
    for column in NUMERIC_COLUMNS:
        original = df[column].copy()
        converted = pd.to_numeric(original, errors="coerce")

        invalid_number = original.notna() & converted.isna()
        add_reason(invalid_number, f"Invalid numeric value: {column}")

        df[column] = converted

    # IDs and quantities must be whole numbers.
    for column in ["order_id", "quantity", "customer_id"]:
        value = df[column]
        fractional = value.notna() & value.mod(1).ne(0)
        add_reason(fractional, f"Must be a whole number: {column}")

    # Validate IDs, quantities and prices.
    add_reason(
        df["order_id"].notna() & df["order_id"].le(0),
        "Order ID must be positive",
    )
    add_reason(
        df["customer_id"].notna() & df["customer_id"].le(0),
        "Customer ID must be positive",
    )
    add_reason(
        df["quantity"].notna() & df["quantity"].le(0),
        "Quantity must be positive",
    )
    add_reason(
        df["unit_price"].notna() & df["unit_price"].lt(0),
        "Unit price cannot be negative",
    )

    # Parse dates and reject invalid ones.
    parsed_dates = pd.to_datetime(
        df["order_date"],
        format="%Y-%m-%d",
        errors="coerce",
    )

    invalid_date = parsed_dates.isna() & df["order_date"].notna()
    add_reason(invalid_date, "Invalid order date")
    df["order_date"] = parsed_dates.dt.strftime("%Y-%m-%d")

    # Keep the first occurrence of each non-missing order ID.
    duplicate = (
        df["order_id"].notna()
        & df["order_id"].duplicated(keep="first")
    )
    add_reason(duplicate, "Duplicate order ID")

    # Split records according to the accumulated rejection reasons.
    rejected_mask = reasons.ne("")

    rejected = df.loc[rejected_mask].copy()
    rejected["rejection_reason"] = reasons.loc[rejected_mask]

    valid = df.loc[~rejected_mask].copy()

    # Calculate sales only for records that passed validation.
    valid["total_amount"] = valid["quantity"] * valid["unit_price"]

    return valid, rejected

#Main method to run the ETL pipeline

def main() -> None:
    logger.info("Sales ETL pipeline started")

    try:
        sales_df = ingest_sales_data(RAW_DATA_PATH)
        logger.info("Ingested %d raw records", len(sales_df))

        valid_df, rejected_df = validate_and_transform(sales_df)

        valid_df.to_csv(PROCESSED_DATA_PATH, index=False)
        rejected_df.to_csv(REJECTED_DATA_PATH, index=False)

        # Step 1: Load the processed CSV into SQLite
        database_count = load_sales_to_database(PROCESSED_DATA_PATH)

        # Step 2: Log the database loading result
        logger.info(
            "Loaded %d sales records into SQLite",
            database_count,
        )

        # Step 3: Report successful completion
        logger.info("Sales ETL pipeline completed successfully")

        print(f"Raw records: {len(sales_df)}")
        print(f"Valid records: {len(valid_df)}")
        print(f"Rejected records: {len(rejected_df)}")
        print(f"Database records: {database_count}")

    except Exception:
        logger.exception("Sales ETL pipeline failed")
        raise


if __name__ == "__main__":
    main()
