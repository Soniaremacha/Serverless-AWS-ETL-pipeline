# Serverless AWS ETL Pipeline (Kaggle + OGD India → Athena)

Fully serverless ETL on AWS that ingests public datasets, stores them in S3 as partitioned Parquet, catalogs them with AWS Glue, and makes them directly queryable with Amazon Athena. Designed to be simple, reproducible, and low-cost.

> **License:** MIT. See [`LICENSE`](./LICENSE).



## Architecture


**Jobs**
- `static_locations_etl`: one-off/enrichment transforms for static location data.
- `weekly_etl`: periodic ingestion + transforms for loans/transactions/customers.

**Automation**
- **EventBridge** schedules the weekly job (and can route job-success events).
- **Lambda** (optional) starts the relevant **Glue Crawlers** after a successful job so Athena sees new partitions immediately.

Repository layout: `Eventbridge/`, `Glue/`, `Lambda/`, `README.md`, `LICENSE`. 



## Features

- 100% serverless (pay-per-use).  
- Partitioned Parquet outputs for fast/cheap queries.  
- Automated catalog updates via Glue Crawlers.  
- Reproducible jobs and clear separation between **static** and **weekly** data.  
- Ready for BI tools like Tableau through Athena.



## Data sources (examples)

- **Kaggle**: Loan applications & transactions (public dataset).  
- **OGD India**: All-India Pincode / administrative directories (public open data).

> Replace with your exact dataset links if you publish them.



## Getting started

### Prerequisites
- AWS account + AWS CLI v2 configured.
- Python 3.10+ (for local packaging if needed).
- An S3 bucket (e.g., `s3://<your-bucket>/tfm/`) with folders:
  - `raw/` (initial drops)
  - `processed/` (Parquet outputs)
- Glue Data Catalog **database** (e.g., `tfm_db`).

### Environment variables (suggested)
- `AWS_REGION` – e.g., `eu-west-1`
- `S3_BUCKET` – your target bucket
- If you ingest from Kaggle via API: `KAGGLE_USERNAME`, `KAGGLE_KEY`



## Deploy & configure (step-by-step)

1. **Create S3 structure**
   - `s3://<bucket>/raw/...`
   - `s3://<bucket>/processed/...` (Glue jobs will write here as Parquet)

2. **Glue Data Catalog**
   - Create database `tfm_db`.

3. **Glue Jobs**
   - Create two jobs, upload scripts from `/Glue/`:
     - `static_locations_etl` (run once or on demand)
     - `weekly_etl` (scheduled)
   - Job role needs access to S3 (read `raw`, write `processed`), Glue (catalog/crawlers), and CloudWatch Logs.

4. **Glue Crawlers**
   - Create crawlers for each processed table (e.g., `loan_applications`, `transactions`, `customers`, `locations`, `centroids`).
   - Target: `s3://<bucket>/processed/<table>/`
   - Database: `tfm_db`

5. **EventBridge scheduling**
   - Create a rule to **run `weekly_etl`** on your desired cadence (e.g., weekly).
   - (Optional) Create a rule for **Glue Job State Change** (success) → target a **Lambda** to start the appropriate crawlers.

6. **Lambda (optional but handy)**
   - Deploy the small function in `/Lambda/` that:
     - Reads the Glue job name from the event.
     - Starts the mapped crawlers **only** when the job is `SUCCEEDED`.

7. **Test**
   - Manually run `static_locations_etl` (first-time enrichment).
   - Manually run `weekly_etl` once to validate the full flow.
   - Confirm crawlers update the Catalog and that you can query in Athena.



## Querying in Athena (examples)

> Adjust database/table/column names to match your final schema.

**1) Fraud rate by state (join with locations)**
```sql
SELECT
  l.state AS state,
  COUNT_IF(a.is_fraud = TRUE) * 1.0 / COUNT(*) AS fraud_rate,
  COUNT(*) AS applications
FROM tfm_db.loan_applications a
JOIN tfm_db.locations l
  ON a.location_state = l.statename
GROUP BY 1
ORDER BY fraud_rate DESC;

```

**2) Monthly approval ratio**
```sql
SELECT
  date_trunc('month', a.application_date) AS month,
  COUNT_IF(a.is_approved = TRUE) * 1.0 / COUNT(*) AS approval_ratio,
  COUNT(*) AS applications
FROM tfm_db.loan_applications a
GROUP BY 1
ORDER BY 1;

```

**3) Transactions flagged vs. total by state**
```sql
SELECT
  l.state,
  COUNT_IF(t.is_flagged = TRUE) AS flagged_tx,
  COUNT(*) AS total_tx,
  COUNT_IF(t.is_flagged = TRUE) * 1.0 / COUNT(*) AS flagged_ratio
FROM tfm_db.transactions t
LEFT JOIN tfm_db.locations l
  ON t.location_state = l.statename
GROUP BY 1
ORDER BY flagged_ratio DESC;

```



## Cost & operations

- Storage: Parquet reduces size and speeds up Athena scans.
- Compute: Glue and crawlers are pay-per-use; EventBridge & Lambda costs are negligible at this scale.
- Monitoring: Use CloudWatch logs/metrics for Glue Jobs and Lambda.



## Security & legal

- Data here references public sources; validate licenses for your exact datasets.
- No real personal data is processed; if you add any, review GDPR implications (PII handling, retention, access control).



## Author

Sonia Remacha — Data Engineering / Analytics.

Repo: Soniaremacha/Serverless-AWS-ETL-pipeline.
