import logging
import os
import pandas as pd
import random
import requests
import sys
import time
import urllib3
from subprocess import run
from datetime import datetime, timedelta

# Path for custom modules to support cleaner separation of intent
sys.path.append('/home/cdsw/')
import constants
import database as db

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Configuration
LLAMA_MODEL = "llama-model-name"
LLAMA_API_URL = "llama-api/v1/chat/completions"
LLAMA_API_KEY = os.getenv("LLAMA_API_KEY", "llama-api-key")

MAX_CHUNKS = 300

OUTPUT_FILE_PATH = "./customer_data/incremental.csv"

# Call Llama API using requests

def call_llama_chat(prompt, retries=5, backoff=10):
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Attempt {attempt}: Sending request to Llama API...")
            headers = {
                "Authorization": f"Bearer {LLAMA_API_KEY}",
                "Content-Type": "application/json",
            }
            data = {
                "model": LLAMA_MODEL,
                "messages": [{"role": "user", "content": prompt}],
            }
            response = requests.post(
                LLAMA_API_URL, headers=headers, json=data, verify=False, timeout=300
            )
            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"]
            elif response.status_code == 429:
                wait_time = backoff * (2 ** (attempt - 1)) + random.uniform(1, 3)
                logging.warning(
                    f"Rate limit reached. Retrying after {wait_time:.2f} seconds..."
                )
                time.sleep(wait_time)
            else:
                logging.error(
                    f"API error: {response.status_code} - {response.text}"
                )
                break
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            time.sleep(backoff * (2 ** (attempt - 1)))
    return "Error: Failed to call Llama API after multiple retries."

# Chunking text
def chunk_text(text, max_tokens=3000):
    """Split text into chunks of appropriate length."""
    words = text.split()
    chunks = []
    current_chunk = []
    current_length = 0

    for word in words:
        if current_length + len(word) + 1 > max_tokens:
            chunks.append(" ".join(current_chunk))
            # current_chunk = []
            current_length = 0
        current_chunk.append(word)
        current_length += len(word) + 1
        
    if current_chunk:
        chunks.append(" ".join(current_chunk))
    return chunks

# Sub-chunking logic
def sub_chunk_and_retry(prompt, max_tokens=3000):
    """Handle API failures by sub-chunking and retrying."""
    chunks = chunk_text(prompt, max_tokens=max_tokens // 2)
    if len(chunks) > MAX_CHUNKS:
        logging.warning(f"Skipping processing as chunks exceed {MAX_CHUNKS}.")
        return "Error: Too many chunks. Skipping."
    responses = []
    for i, chunk in enumerate(chunks):
        logging.info(f"Sub-chunk {i + 1}/{len(chunks)} being processed...")
        response = call_llama_chat(chunk)
        if "Error:" in response:
            logging.warning("Sub-chunk failed. Skipping...")
            continue
        responses.append(response)
    return " ".join(responses)
  
# Define a list of customers to skip
CUSTOMERS_TO_SKIP = [
    "customer-name"
]

def generate_summary(df):
    print(df)
    """Generate summaries for each customer."""
    grouped = df.groupby("customer")
    os.makedirs("./customer_data", exist_ok=True)

    for customer, group in grouped:
        # Check if the customer is in the skip list
        if customer in CUSTOMERS_TO_SKIP:
            logging.info(f"Skipping customer: {customer}")
            continue

        try:
            
            logging.info(f"Processing customer: {customer}")
            group["case_creation_date"] = pd.to_datetime(group["case_creation_date"])

            # Gather issues for the quarter and week
            quarter_issues = "\n".join(group["case_description"])
            one_week_ago = datetime.now() - timedelta(days=7)
            weekly_issues = "\n".join(
                group[group["case_creation_date"] >= one_week_ago]["case_description"]
            )

            # Ensure quarter summary fits within model limits
            quarter_chunks = chunk_text(quarter_issues, max_tokens=3000)  # Adjust based on model context limit
            logging.info("made it 2")
            quarter_summary = call_llama_chat(f"""
            Customer: {customer}
            Issues this quarter: {" ".join(quarter_chunks)}

            Summarize the customer's issues this quarter in a concise paragraph.Use Case number as references and priority of the case found in the information for the customer's cases to provide a good summary.
            """)
            logging.info(f"Populated quarter issues for {customer}")
            if "Error:" in quarter_summary:
                logging.warning("Quarter summary generation failed. Retrying with sub-chunking...")
                quarter_summary = sub_chunk_and_retry(quarter_issues, max_tokens=3000)

            # Ensure weekly summary fits within model limits
            weekly_chunks = chunk_text(weekly_issues, max_tokens=3000)
            weekly_summary = call_llama_chat(f"""
            Customer: {customer}
            Issues this week: {" ".join(weekly_chunks)}

            Summarize the customer's issues this week in a concise paragraph. Use Case number as references and priority of the case found in the information for the customer's cases to provide a good summary.
            """)
            logging.info(f"Populated quarter issues for {customer}")
            if "Error:" in weekly_summary:
                logging.warning("Weekly summary generation failed. Retrying with sub-chunking...")
                weekly_summary = sub_chunk_and_retry(weekly_issues, max_tokens=3000)

            # Generate use cases
            use_cases = call_llama_chat(f"""
            Customer: {customer}
            Quarter Summary: {quarter_summary}

            Describe the customer's use cases for Cloudera.
            """)
            logging.info("Use cases logged for {customer}")
            if "Error:" in use_cases:
                logging.warning(f"Use cases generation failed for {customer}. Retrying with sub-chunking...")
                use_cases = sub_chunk_and_retry(
                    f"Customer: {customer}\nQuarter Summary: {quarter_summary}\n\nDescribe the customer's use cases for Cloudera."
                )

            # Generate Cloudera components
            cloudera_components = call_llama_chat(f"""
            Customer: {customer}
            Quarter Summary: {quarter_summary}

            List the Cloudera components the customer is using.
            """)
            if "Error:" in cloudera_components:
                logging.warning(f"Cloudera components generation failed for {customer}. Retrying with sub-chunking...")
                cloudera_components = sub_chunk_and_retry(
                    f"Customer: {customer}\nQuarter Summary: {quarter_summary}\n\nList the Cloudera components the customer is using."
                )
            logging.info("Components logged for {customer}")

            # Generate sales opportunities
            sales_opportunities = call_llama_chat(f"""
            Customer: {customer}
            Quarter Summary: {quarter_summary}

            Suggest potential sales opportunities based on Cloudera technologies.
            """)
            if "Error:" in sales_opportunities:
                logging.warning(f"Sales opportunities generation failed for {customer}. Retrying with sub-chunking...")
                sales_opportunities = sub_chunk_and_retry(
                    f"Customer: {customer}\nQuarter Summary: {quarter_summary}\n\nSuggest potential sales opportunities based on Cloudera technologies."
                )
            logging.info("Sales opps logged for {customer}")    
            summary = {
                "customer": [customer],
                "quarter_summary": [quarter_summary.strip() if quarter_summary else "Not Available"],
                "week_summary": [weekly_summary.strip() if weekly_summary else "Not Available"],
                "use_cases": [use_cases.strip() if use_cases else "Not Available"],
                "cloudera_components": [cloudera_components.strip() if cloudera_components else "Not Available"],
                "sales_opportunities": [sales_opportunities.strip() if sales_opportunities else "Not Available"],
            }
            df = pd.DataFrame(summary)
            df.to_csv(OUTPUT_FILE_PATH, header=0, index=False, sep="|", mode="a")
            
            logging.info("Incremental processed data saved to ./customer_data/incremental_output.csv")
            # Add a delay to prevent rate-limiting
            time.sleep(5)
        except Exception as e:
            logging.error(f"Error processing customer {customer}: {e}")
            print("testing error exception")
               
        
# Main function
if __name__ == "__main__":
    try:
        # df = db.fetch_data_from_impala(constants.IMPALA_QUERY, save_local=True)
        # db.create_impala_table()
        # Retrieve data locally to ease development
        df = pd.read_csv('./customer_data/query_output.csv', header=0)
        
        summaries = generate_summary(df)
        # save_final_data(summaries)
        
        # upload_csv_to_hdfs("./customer_data/final_output.csv", "/user/nraj/final_output.csv")
        # load_csv_to_impala("/user/nraj/final_output.csv")
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
