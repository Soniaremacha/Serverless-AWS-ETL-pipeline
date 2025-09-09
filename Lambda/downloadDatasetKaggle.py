import os
import boto3
import zipfile

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Environment configuration
    os.environ['KAGGLE_CONFIG_DIR'] = '/tmp/.kaggle'
    os.makedirs('/tmp/.kaggle', exist_ok=True)
    os.makedirs('/tmp/kaggle_data', exist_ok=True)

    # Download kaggle.json from S3
    s3.download_file('sonia-uned-tfm-bucket', 'secrets/kaggle.json', '/tmp/.kaggle/kaggle.json')

    # Import kaggle.json
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()

    # Download dataset and extract
    api.dataset_download_files(
        dataset='prajwaldongre/loan-application-and-transaction-fraud-detection',
        path='/tmp/kaggle_data',
        unzip=True
    )

    # Load files to S3
    for file in ["transactions.csv", "loan_applications.csv"]:
        local = f"/tmp/kaggle_data/{file}"
        if os.path.exists(local):
            s3.upload_file(local, "sonia-uned-tfm-bucket", f"kaggle-data/{file}")

    return {
        "statusCode": 200,
        "body": "Archivos descargados y subidos correctamente a S3."
    }
