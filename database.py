import logging
from impala.dbapi import connect
from impala.error import HiveServer2Error

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

IMPALA_HOST = os.getenv("IMPALA_HOST", "impala-host")
IMPALA_PORT = int(os.getenv("IMPALA_PORT", impala-port-int))
KERBEROS_SERVICE_NAME = "impala"

# Fetch data from Impala
def fetch_data_from_impala(query, save_local=True):
    """Fetch data from Impala and optionally save locally as CSV."""
    conn = None
    try:
        logging.info("Connecting to Impala...")
        conn = connect(
            host=IMPALA_HOST,
            port=IMPALA_PORT,
            auth_mechanism="GSSAPI",
            kerberos_service_name=KERBEROS_SERVICE_NAME,
            use_ssl=True,
        )
        cursor = conn.cursor()
        logging.info("Executing query...")
        cursor.execute(query)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(result, columns=columns)
        if save_local:
            os.makedirs("./customer_data", exist_ok=True)
            df.to_csv("./customer_data/query_output.csv", index=False)
            logging.info("Query output saved to ./customer_data/query_output.csv")
        return df
    except HiveServer2Error as e:
        logging.error(f"ImpalaServer2Error: {e}")
        raise
    finally:
        if conn != None:
         conn.close()
        
def create_impala_table():
    try:
        conn = connect(
            host=IMPALA_HOST,
            port=IMPALA_PORT,
            auth_mechanism="GSSAPI",
            kerberos_service_name=KERBEROS_SERVICE_NAME,
            use_ssl=True,
        )
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS u_nraj.llm_output (
                customer STRING,
                quarter_summary STRING,
                week_summary STRING,
                use_cases STRING,
                cloudera_components STRING,
                sales_opportunities STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """)
        conn.close()
        logging.info("Ensured Impala table exists.")
    except HiveServer2Error as e:
        logging.error(f"Error ensuring Impala table exists: {e}")

# Insert Data into Impala
def insert_into_impala(summary):
    try:
        conn = connect(
            host=IMPALA_HOST,
            port=IMPALA_PORT,
            auth_mechanism="GSSAPI",
            kerberos_service_name=KERBEROS_SERVICE_NAME,
            use_ssl=True,
        )
        cursor = conn.cursor()
        insert_query = f"""
        INSERT INTO u_nraj.llm_output VALUES (
            '{summary["customer"]}',
            '{summary["quarter_summary"]}',
            '{summary["week_summary"]}',
            '{summary["use_cases"]}',
            '{summary["cloudera_components"]}',
            '{summary["sales_opportunities"]}'
        )
        """
        cursor.execute(insert_query)
        conn.close()
        logging.info(f"Inserted processed customer into Impala: {summary['customer']}")
    except HiveServer2Error as e:
        logging.error(f"Error inserting customer {summary['customer']} into Impala: {e}")

# Load CSV into Impala
def load_csv_to_impala(hdfs_path):
    """Load the CSV file from HDFS into the Impala table."""
    try:
        logging.info("Connecting to Impala...")
        conn = connect(
            host=IMPALA_HOST,
            port=IMPALA_PORT,
            auth_mechanism="GSSAPI",
            kerberos_service_name=KERBEROS_SERVICE_NAME,
            use_ssl=True,
        )
        cursor = conn.cursor()

        # Drop and recreate table
        logging.info("Recreating the Impala table...")
        cursor.execute("DROP TABLE IF EXISTS u_nraj.llm_output")
        cursor.execute("""
            CREATE TABLE u_nraj.llm_output (
                customer STRING,
                quarter_summary STRING,
                week_summary STRING,
                use_cases STRING,
                cloudera_components STRING,
                sales_opportunities STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """)

        # Load data into the table
        load_query = f"""
            LOAD DATA INPATH '{hdfs_path}' INTO TABLE u_nraj.llm_output
        """
        logging.info("Loading data into Impala table...")
        cursor.execute(load_query)
        conn.close()
        logging.info("Data loaded into Impala table successfully.")
    except HiveServer2Error as e:
        logging.error(f"ImpalaServer2Error: {e}")
        raise
