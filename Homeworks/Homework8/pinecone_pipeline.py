from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.models import Variable
import pandas as pd
import time
import requests
import os
import json

from sentence_transformers import SentenceTransformer
from pinecone import Pinecone, ServerlessSpec

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='Customer_Orders_to_Pinecone',
    default_args=default_args,
    description='Index customer orders CSV into Pinecone for semantic search',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['orders', 'pinecone', 'semantic-search'],
) as dag:
    """
    DAG to build a semantic search index over a customer orders CSV using Pinecone.
    """

    @task
    def download_data():
        """Download customer orders dataset using requests"""
        data_dir = '/tmp/medium_data'
        os.makedirs(data_dir, exist_ok=True)

        file_path = f"{data_dir}/customer-orders.csv"
        url = 'https://s3-geospatial.s3.us-west-2.amazonaws.com/customer-orders.csv'  # updated source

        resp = requests.get(url, stream=True, timeout=60)
        if resp.status_code == 200:
            with open(file_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            # quick sanity check
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                line_count = sum(1 for _ in f)
            print(f"Downloaded file has {line_count} lines")
        else:
            raise Exception(f"Failed to download data: HTTP {resp.status_code}")

        return file_path

    @task
    def preprocess_data(data_path):
        """Clean and prepare customer order data for embedding"""
        df = pd.read_csv(data_path)

        # Prefer a stable ID if present
        id_col = None
        for candidate in ['order_id', 'OrderID', 'id', 'ID']:
            if candidate in df.columns:
                id_col = candidate
                break

        # Identify likely text-bearing columns (object dtype)
        text_cols = [c for c in df.columns if df[c].dtype == 'object']
        # Optionally exclude some columns if needed (e.g., long free-form PII)
        excluded = set([])
        text_cols = [c for c in text_cols if c not in excluded]
        if not text_cols:
            text_cols = list(df.columns)

        # Build a single free-text field; join "colname: value"
        def row_to_text(row):
            parts = []
            for c in text_cols:
                val = row.get(c, "")
                if pd.notna(val) and str(val).strip():
                    parts.append(f"{c}: {val}")
            return " | ".join(parts)

        df['text'] = df.apply(row_to_text, axis=1)

        # Minimal, useful metadata (keep small)
        likely_meta_keys = [k for k in ['order_id', 'customer_id', 'product', 'category', 'status', 'notes']
                            if k in df.columns]

        def row_to_meta(row):
            meta = {}
            for k in likely_meta_keys:
                v = row.get(k, None)
                if pd.notna(v):
                    meta[k] = str(v)
            preview = row.get('text', '')
            meta['preview'] = (preview[:300] + '…') if isinstance(preview, str) and len(preview) > 300 else preview
            return meta

        df['metadata_json'] = df.apply(lambda r: json.dumps(row_to_meta(r)), axis=1)

        # Final ID column
        if id_col:
            df['id'] = df[id_col].astype(str)
        else:
            df['id'] = df.reset_index(drop=True).index.astype(str)

        preprocessed_path = '/tmp/medium_data/customer-orders-preprocessed.csv'
        df[['id', 'text', 'metadata_json']].to_csv(preprocessed_path, index=False)
        print(f"Preprocessed data saved to {preprocessed_path}")
        return preprocessed_path

    @task
    def create_pinecone_index():
        """Create or reset Pinecone index"""
        api_key = Variable.get("pinecone_api_key")
        pc = Pinecone(api_key=api_key)

        spec = ServerlessSpec(cloud="aws", region="us-east-1")
        index_name = 'semantic-search-fast'

        # Delete if exists
        existing = [ix.name if hasattr(ix, "name") else (ix.get("name") if isinstance(ix, dict) else str(ix))
                    for ix in pc.list_indexes()]
        if index_name in existing:
            pc.delete_index(index_name)

        # Create new index
        pc.create_index(
            name=index_name,
            dimension=384,   # all-MiniLM-L6-v2
            metric='dotproduct',
            spec=spec
        )

        # Wait for ready
        while True:
            desc = pc.describe_index(index_name)
            status = getattr(desc, "status", None) or desc.get("status", {})
            if status.get("ready", False):
                break
            time.sleep(1)

        print(f"Pinecone index '{index_name}' created successfully")
        return index_name

    @task
    def generate_embeddings_and_upsert(data_path, index_name):
        """Generate embeddings from 'text' field and upsert to Pinecone"""
        api_key = Variable.get("pinecone_api_key")

        df = pd.read_csv(data_path)
        model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')

        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)

        batch_size = 100
        total = len(df)
        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)
            batch = df.iloc[start:end].copy()

            texts = batch['text'].fillna("").astype(str).tolist()
            embeddings = model.encode(texts)

            vectors = []
            for j, (_, row) in enumerate(batch.iterrows()):
                try:
                    meta = json.loads(row['metadata_json']) if pd.notna(row['metadata_json']) else {}
                except Exception:
                    meta = {}
                vectors.append({
                    'id': str(row['id']),
                    'values': embeddings[j].tolist(),
                    'metadata': meta
                })

            # Upsert
            index.upsert(vectors=vectors)
            print(f"Upserted {end} / {total}")

        print(f"Successfully upserted {total} records to Pinecone")
        return index_name

    @task
    def test_search_query(index_name):
        """Test a sample semantic search against the orders data"""
        api_key = Variable.get("pinecone_api_key")
        model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')

        pc = Pinecone(api_key=api_key)
        index = pc.Index(index_name)

        query = "orders with refund or return issues"
        qvec = model.encode(query).tolist()

        results = index.query(vector=qvec, top_k=5, include_metadata=True)

        print(f"Search results for query: '{query}'")
        matches = results.get('matches', []) if isinstance(results, dict) else getattr(results, 'matches', [])
        for m in matches:
            mid = m.get('id', '')
            score = m.get('score', 0.0)
            meta = m.get('metadata', {}) or {}
            preview = meta.get('preview', '')
            print(f"ID: {mid} | Score: {score:.4f} | Preview: {preview[:120]}...")

    # Define task dependencies using the TaskFlow API
    data_path = download_data()
    preprocessed_path = preprocess_data(data_path)
    index_name = create_pinecone_index()
    final_index_name = generate_embeddings_and_upsert(preprocessed_path, index_name)
    test_search_query(final_index_name)
