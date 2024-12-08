import sys
import pandas as pd
import psycopg2
import logging

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,  # Set logging level to DEBUG for detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='/home/ubuntu/airflow/logs/pipeline_test.log',
    filemode='a',
)

try:
    ACTIVE_ADVERTISERS_FILE = "/home/ubuntu/advertiser_ids.csv"
    PRODUCT_LOG_FILE = "/home/ubuntu/product_views.csv"
    ADS_LOG_FILE = "/home/ubuntu/ads_views.csv"
    POSTGRES_URI = "postgresql://postgres:nicosofivlad@grupo-18-rds2.cf4i6e6cwv74.us-east-1.rds.amazonaws.com/postgres"

    # Parse execution date
    if len(sys.argv) < 2:
        logging.error("No execution_date provided as an argument.")
        raise ValueError("Execution date is required as the first argument.")
    
    execution_date = sys.argv[1]
    execution_date = pd.to_datetime(execution_date)
    logging.info(f"Execution date parsed: {execution_date}")

    # Define helper functions
    def filter_data(active_advertisers, log_data, execution_date=None):
        """Filter logs based on active advertisers and date."""
        log_data['date'] = pd.to_datetime(log_data['date'])
        logging.debug("Filtering data based on execution date and active advertisers.")
        if execution_date:
            log_data = log_data[log_data['date'] == execution_date]
        filtered_data = log_data[log_data['advertiser_id'].isin(active_advertisers)]
        logging.debug(f"Filtered data: {filtered_data.shape[0]} rows")
        return filtered_data

    # Read active advertisers
    logging.info("Reading active advertisers file.")
    active_advertisers = pd.read_csv(ACTIVE_ADVERTISERS_FILE)['advertiser_id'].tolist()

    # Load raw logs
    logging.info("Loading product and ads log files.")
    product_views = pd.read_csv(PRODUCT_LOG_FILE)
    ads_views = pd.read_csv(ADS_LOG_FILE)

    # Filter logs
    filtered_product_views = filter_data(active_advertisers, product_views, execution_date)
    filtered_ads_views = filter_data(active_advertisers, ads_views, execution_date)

    def compute_top_ctr(ads_data, top_n=20):
        """Compute TopCTR model, only including CTR > 0."""
        logging.debug("Computing TopCTR model.")
        ctr_data = ads_data.groupby(['advertiser_id', 'product_id', 'type']).size().unstack(fill_value=0)
        
        # Avoid division by zero by setting ctr to 0 where impressions are zero
        ctr_data['ctr'] = ctr_data.get('click', 0) / ctr_data.get('impression', 1)
        
        # Replace Infinity or NaN values with 0 or another value
        ctr_data['ctr'].replace([float('inf'), float('-inf')], 0, inplace=True)
        ctr_data['ctr'].fillna(0, inplace=True)
        
        # Filter out rows with ctr <= 0
        ctr_data = ctr_data[ctr_data['ctr'] > 0]
        
        # Get the top N per advertiser
        top_ctr = ctr_data.sort_values(by='ctr', ascending=False).groupby('advertiser_id').head(top_n)
        logging.debug(f"TopCTR computed: {top_ctr.shape[0]} rows")
        return top_ctr.reset_index()
    
    # Compute TopCTR
    logging.info("Computing TopCTR results.")
    top_ctr_results = compute_top_ctr(filtered_ads_views)

    def compute_top_products(product_data, top_n=20):
        """Compute TopProduct model, only including products with views > 0."""
        logging.debug("Computing TopProducts model.")
        view_counts = product_data.groupby(['advertiser_id', 'product_id']).size().reset_index(name='views')
        
        # Filter out products with views <= 0
        view_counts = view_counts[view_counts['views'] > 0]
        
        # Get the top N products per advertiser
        top_products = view_counts.sort_values(by='views', ascending=False).groupby('advertiser_id').head(top_n)
        logging.debug(f"TopProducts computed: {top_products.shape[0]} rows")
        return top_products

    # Compute TopProduct
    logging.info("Computing TopProduct results.")
    top_product_results = compute_top_products(filtered_product_views)

    def write_to_db_psycopg2(df, table_name, execution_date):
        """Write data to PostgreSQL."""
        logging.info(f"Writing data to PostgreSQL table: {table_name}.")
        conn = psycopg2.connect(
            database="postgres",
            user="postgres",
            password="nicosofivlad",
            host="grupo-18-rds2.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
            port="5432"
        )
        cursor = conn.cursor()

        if table_name == "top_ctr":
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                advertiser_id VARCHAR(255),
                product_id VARCHAR(255),
                ctr FLOAT,
                date DATE
            );
            """
        elif table_name == "top_products":
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                advertiser_id VARCHAR(255),
                product_id VARCHAR(255),
                views INT,
                date DATE
            );
            """
        else:
            raise ValueError("Unsupported table name")

        cursor.execute(create_table_query)
        for index, row in df.iterrows():
            if table_name == "top_ctr":
                query = f"INSERT INTO {table_name} (advertiser_id, product_id, ctr, date) VALUES (%s, %s, %s, %s)"
                cursor.execute(query, (str(row['advertiser_id']), str(row['product_id']), row['ctr'], execution_date))
            elif table_name == "top_products":
                query = f"INSERT INTO {table_name} (advertiser_id, product_id, views, date) VALUES (%s, %s, %s, %s)"
                cursor.execute(query, (str(row['advertiser_id']), str(row['product_id']), row['views'], execution_date))

        conn.commit()
        cursor.close()
        conn.close()

    # Write results to database
    write_to_db_psycopg2(top_ctr_results, "top_ctr", execution_date)
    write_to_db_psycopg2(top_product_results, "top_products", execution_date)

    logging.info("Pipeline completed successfully.")

except Exception as e:
    logging.error(f"An error occurred: {e}")
    raise
