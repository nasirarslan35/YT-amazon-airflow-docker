import boto3
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from datetime import datetime

def check_and_extract_data():
    # S3 and database connection details
    s3_bucket_name = 'airflow-basic035'
    s3_object_key = 'initial_upload/training.1600000.processed.noemoticon_1.csv'
    db_engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')

    # Function for tables exist
    def check_and_create_table(conn):
        conn.execute("""
        CREATE TABLE IF NOT EXISTS s3_file_data (
            id SERIAL PRIMARY KEY,
            file_last_modified TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS tweets (
            id SERIAL PRIMARY KEY,           -- Unique identifier for the table
            tweet_id varchar(100),          -- Unique ID of the tweet
            target SMALLINT,                 -- Polarity of the tweet (0, 2, 4)
            date varchar(500),                  -- Date of the tweet
            flag TEXT,                       -- Query flag (e.g., NO_QUERY)
            username TEXT,                   -- User who tweeted
            text TEXT,                       -- Content of the tweet
            created_at TIMESTAMP DEFAULT NOW() -- Timestamp of record creation
        );
        """)

    # check tables exist
    with db_engine.connect() as conn:
            table_name = 'tweets'
            table_exists = conn.execute(f"""
            SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = '{table_name}'
            );
            """).scalar()
            if table_exists:
                tweet_count = conn.execute("SELECT COUNT(tweet_id) FROM tweets").scalar()
                file_last_modified_count = conn.execute("SELECT COUNT(file_last_modified) FROM s3_file_data").scalar()
                if tweet_count >= 0 and file_last_modified_count >= 0:
                    print(tweet_count)
                    print(file_last_modified_count)
            else:
                check_and_create_table(conn)

    # Connect to S3
    s3_client = boto3.client(
        service_name='s3',
        aws_access_key_id='AKIA2LIPZ4X2B3QBCFPX',
        aws_secret_access_key='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',
    )

    # Get metadata of file
    response = s3_client.head_object(Bucket=s3_bucket_name, Key=s3_object_key)
    file_last_modified = response['LastModified']
    file_last_modified=file_last_modified.replace(tzinfo=None)
    print (file_last_modified)

    # Check the database for existing data
    with db_engine.connect() as conn:
        result = conn.execute(
            "SELECT file_last_modified FROM s3_file_data ORDER BY id DESC LIMIT 1"
        ).fetchone()

        if result and result[0] >= file_last_modified:
            print("No new data to load.")
              # Insert the file_last_modified into s3_file_data table
            conn.execute(
                "INSERT INTO s3_file_data (file_last_modified) VALUES (%s)",
                (file_last_modified)
            )
            print(f"Inserted file_last_modified: {file_last_modified} into s3_file_data.")
            return
        else:
        
            # Download the file
            response = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_object_key)
            csv_content = response['Body'].read().decode('ISO-8859-1')

            # Load the CSV content into a DataFrame
            df = pd.read_csv(StringIO(csv_content), header=None, names=['target', 'tweet_id', 'date', 'flag', 'username', 'text'])
            df['tweet_id'] = df['tweet_id'].astype(str)

            # Filter out existing tweet_ids
            with db_engine.connect() as conn:
                existing_tweet_ids = pd.read_sql("SELECT tweet_id FROM tweets", conn)['tweet_id'].tolist()
            new_data = df[~df['tweet_id'].astype(str).isin(existing_tweet_ids)]
            # Insert new rows into the database in parts
            chunk_size = 10000
            with db_engine.connect() as conn:
                for i in range(0, len(new_data), chunk_size):
                    chunk = new_data.iloc[i:i + chunk_size]
                    chunk.to_sql('tweets', conn, if_exists='append', index=False)
                    
            with db_engine.connect() as conn:
            # Insert the file_last_modified into s3_file_data
                conn.execute("INSERT INTO s3_file_data (file_last_modified) VALUES (%s)",(file_last_modified))
                try:
                    print(result[0])
                except:
                    print(result)

            print(f"Inserted {len(new_data)} new tweets successfully.")
            
def transform():
    # Database connection details
    db_engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')

    # Fetch data from the tweets table
    with db_engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM tweets", conn)

    if df.empty:
        print("No data in the tweets table.")
        return

    # Assign headers
    df.columns = ["id", "tweet_id", "sentiment", "date", "query", "user", "text", "created_at"]

    # months to their numeric value mappings
    month_map = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
        'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    }

    # Extract components
    df['month'] = df['date'].str[4:7].map(month_map)  # Map month
    df['day'] = df['date'].str[8:10]                 # day
    df['year'] = df['date'].str[-4:]                 # year

    # MM-DD-YYYY format -combine
    df['formatted_date'] = df['month'] + '-' + df['day'] + '-' + df['year']

    # Drop unnecessary columns
    df = df.drop(columns=['month', 'day', 'year', 'date', 'created_at', 'id','sentiment','query'])

    # Filter rows for users
    #filtered_df = df[(df['user'] == 'KatyPerry') | (df['user'] == 'ElonMusk')]
    filtered_df = df[df['user'].isin(['KatyPerry', 'prettiebillie'])]

    with db_engine.connect() as conn:
        # Create table if not exists
        conn.execute("""
        CREATE TABLE IF NOT EXISTS required_tweets (
            tweet_id VARCHAR(100) PRIMARY KEY,
            "user" TEXT,
            text TEXT,
            formatted_date VARCHAR(50)
        );
        """)
        # get existing tweet_ids from the target table
        existing_tweet_ids = pd.read_sql("SELECT tweet_id FROM required_tweets", conn)['tweet_id'].tolist()

    # Filter out rows that already exist in the target table
    new_data = filtered_df[~filtered_df['tweet_id'].isin(existing_tweet_ids)]
    chunk_size = 10000
    with db_engine.connect() as conn:
            for i in range(0, len(new_data), chunk_size):
                chunk = new_data.iloc[i:i + chunk_size]
                chunk.to_sql('required_tweets', conn, if_exists='append', index=False)

    print(f"Inserted {len(new_data)} new rows into required_tweets.")


def load_data():
    # Database connection
    db_engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')

    # S3 connection details
    s3_bucket_name = 'airflow-basic035'
    s3_object_key = f"output/required_tweets_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    aws_access_key_id = 'AKIA2LIPZ4X2B3QBCFPX'
    aws_secret_access_key = '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'

    # get data from the 'required_tweets' table
    with db_engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM required_tweets", conn)

    if df.empty:
        print("No data found in the required_tweets table.")
        return

    # Convert to a CSv
    csv = StringIO()
    df.to_csv(csv, index=False)
    csv.seek(0)

    # Upload CSV to S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    try:
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=s3_object_key,
            Body=csv.getvalue()
        )
        print(f"Data successfully uploaded to s3://{s3_bucket_name}/{s3_object_key}")
    except Exception as e:
        print(f"Error uploading data to S3: {e}")


            
            


