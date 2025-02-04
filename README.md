# GenAI Customer Metrics

## Overview
The **GenAI Customer Metrics** project automates customer issue tracking, summarization, and insights generation using GenAI and Impala. It fetches customer data from Impala, processes it with AI-based summarization, and stores the results for further analysis.

## Features
- **Data Extraction**: Fetches customer support cases from Impala.
- **AI Summarization**: Uses Llama API for generating concise issue summaries.
- **Chunking & Retrying**: Splits long texts into manageable pieces for API calls.
- **Impala Integration**: Stores processed summaries in Impala tables.
- **CSV Storage**: Saves processed data locally in CSV format.

## Installation
### Prerequisites
- Python 3.8+
- Impala Python library (`impyla`)
- Required dependencies:
  ```sh
  pip install -r requirements.txt

## Clone Repository

```
git clone https://github.com/Nishant-K-Raj/genAI-customer-metrics.git
cd genAI-customer-metrics
```

## Set Environment Variables

```
export LLAMA_API_KEY="your_llama_api_key"
export IMPALA_HOST="your_impala_host"
export IMPALA_PORT="your_impala_port"
```

## Usage
### Run the Script

```
python customer_summary.py
```

## Configuration
LLAMA_MODEL: Specifies the AI model used for summarization.
IMPALA_HOST & IMPALA_PORT: Define the connection settings for Impala.
MAX_CHUNKS: Controls the maximum chunks sent to Llama API.

## Output
Processed Summaries: Stored in ./customer_data/final_output.csv
Impala Table: Results are inserted into u_nraj.llm_output

## Troubleshooting
Connection Issues: Check Impala configurations and authentication settings.
API Failures: Ensure LLAMA_API_KEY is valid and not rate-limited.




