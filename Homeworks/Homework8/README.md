# Customer Orders Semantic Search using Airflow & Pinecone

This project builds a **semantic search engine** on top of a CSV dataset (`customer-orders.csv`) hosted on S3.  
It uses **Apache Airflow** to orchestrate the workflow and **Pinecone** as the vector database to store embeddings for similarity search.

---

## Overview

The Airflow DAG automates the following pipeline:

1. **Download dataset** – Fetch `customer-orders.csv` from S3.  
2. **Preprocess data** – Clean and combine text columns into a single searchable field.  
3. **Create Pinecone index** – Initialize or recreate a Pinecone serverless index.  
4. **Generate embeddings** – Encode the text column using `sentence-transformers` (`all-MiniLM-L6-v2`).  
5. **Upsert vectors to Pinecone** – Upload all embeddings in batches.  
6. **Test query** – Run a sample semantic search query (e.g. “orders with refund or return issues”).

---

## Architecture

```
 ┌────────────────────────────┐
 │  Airflow DAG (Customer_Orders_to_Pinecone) │
 └────────────┬───────────────┘
              │
              ▼
  ┌──────────────────────────┐
  │ download_data()          │ → Fetch dataset from S3
  ├──────────────────────────┤
  │ preprocess_data()        │ → Clean, join text fields
  ├──────────────────────────┤
  │ create_pinecone_index()  │ → Create or reset Pinecone index
  ├──────────────────────────┤
  │ generate_embeddings_and_upsert() │ → Encode + batch upload to Pinecone
  ├──────────────────────────┤
  │ test_search_query()      │ → Run example semantic search
  └──────────────────────────┘
```

---

## Tech Stack

| Component | Purpose |
|------------|----------|
| **Apache Airflow** | Task orchestration and scheduling |
| **Pandas** | CSV data preprocessing |
| **SentenceTransformers** | Generating text embeddings |
| **Pinecone** | Vector database for semantic search |
| **Requests** | Downloading dataset from S3 |

---

## Dataset

The DAG fetches this dataset automatically:

```
https://s3-geospatial.s3.us-west-2.amazonaws.com/customer-orders.csv
```

It contains sample order information (order IDs, customer details, etc.).

---

##  Prerequisites

1. **Python 3.9+**
2. **Airflow environment** (LocalExecutor or Docker)
3. **Pinecone account**

### Install dependencies

```bash
pip install apache-airflow pandas requests sentence-transformers pinecone-client
```

---

## How to Run

1. Place the DAG file (`pinecone_pipeline.py`) into your Airflow `dags/` folder.  
2. Ensure Airflow webserver and scheduler are running.  
3. In the Airflow UI, trigger the DAG:  
   ```
   DAGs → Customer_Orders_to_Pinecone → Trigger DAG
   ```
4. Monitor logs to confirm each stage completes:
   - ✅ Data downloaded  
   - ✅ Preprocessed CSV created  
   - ✅ Pinecone index created  
   - ✅ Vectors upserted  
   - ✅ Search results displayed

---

## Example Search Output

```
Search results for query: 'orders with refund or return issues'
ID: 12543 | Score: 0.8123 | Preview: order_id: 12543 | product: Headphones | notes: refund requested...
ID: 9821  | Score: 0.8076 | Preview: order_id: 9821 | product: Laptop | status: returned | ...
...
```
---

## File Structure

```
dags/
├── pinecone_pipeline.py       # Main Airflow DAG
├── logs/                      # Airflow runtime logs
└── README.md                  # This file
```

---

## 🧠 Learn More

- [Pinecone Docs](https://docs.pinecone.io)
- [SentenceTransformers Models](https://www.sbert.net)
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
