import psycopg2
import pandas as pd
from fastapi import FastAPI, HTTPException
from datetime import datetime, timedelta

app = FastAPI()

# Database connection details
POSTGRES_URI = "postgresql://postgres:nicosofivlad@grupo-18-rds2.cf4i6e6cwv74.us-east-1.rds.amazonaws.com/postgres"

def get_db_connection():
    # Establish a connection to PostgreSQL using psycopg2
    return psycopg2.connect(
        database="postgres",
        user="postgres",
        password="nicosofivlad",
        host="grupo-18-rds2.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
        port="5432"
    )

@app.get("/recommendations/{adv}/{model}")
def get_recommendations(adv: str, model: str):
    """
    Fetch recommendations for a given advertiser and model.
    """
    valid_models = ["top_ctr", "top_products"]
    if model not in valid_models:
        raise HTTPException(status_code=400, detail=f"Model '{model}' is invalid. Use 'top_ctr' or 'top_products'.")

    try:
        # Get the database connection
        conn = get_db_connection()

        # Prepare the query, filtering by advertiser ID and today's date
        query = f"SELECT * FROM {model} WHERE advertiser_id = %s AND date = %s;"
        today = datetime.now().strftime('%Y-%m-%d')

        # Log the query to verify correctness
        print(f"Executing query: {query} with params: {adv}, {today}")

        # Fetch the data using pandas with the psycopg2 connection
        df = pd.read_sql(query, conn, params=(adv, today))

        # Close the connection
        conn.close()

        # Log if no data is found
        if df.empty:
            print(f"No recommendations found for advertiser {adv} on {today}.")
            raise HTTPException(status_code=404, detail="No recommendations found for the given advertiser and model.")

        # Return the results as a JSON object
        return df.to_dict(orient="records")

    except Exception as e:
        # Log the error for debugging
        print(f"Error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/")
def get_stats():
    """
    Provide statistics about recommendations.
    """
    try:
        conn = get_db_connection()

        # Example stats:
        # 1. Total advertisers
        total_advertisers_query = "SELECT COUNT(DISTINCT advertiser_id) FROM top_ctr;"
        total_advertisers = int(pd.read_sql(total_advertisers_query, conn).iloc[0, 0])  # Explicit conversion to int

        # 2. Advertisers with most variation
        variation_query = """
        SELECT advertiser_id, COUNT(DISTINCT product_id) AS variation
        FROM top_ctr
        GROUP BY advertiser_id
        ORDER BY variation DESC
        LIMIT 5;
        """
        variations = pd.read_sql(variation_query, conn).to_dict(orient="records")

        # 3. Model overlap stats
        overlap_query = """
        SELECT COUNT(*) AS matches
        FROM top_ctr AS ctr
        JOIN top_products AS prod
        ON ctr.advertiser_id = prod.advertiser_id
        AND ctr.product_id = prod.product_id
        AND ctr.date = prod.date;
        """
        overlap_matches = int(pd.read_sql(overlap_query, conn).iloc[0, 0])  # Explicit conversion to int

        conn.close()

        return {
            "total_advertisers": total_advertisers,
            "top_variations": variations,
            "model_overlap": overlap_matches,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/history/{adv}/")
def get_history(adv: str):
    """
    Provide recommendations for the last 7 days for a given advertiser.
    """
    try:
        conn = get_db_connection()
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        query = """
        SELECT *
        FROM (
            SELECT * FROM top_ctr
            UNION
            SELECT * FROM top_products
        ) AS combined
        WHERE advertiser_id = %s AND date >= %s
        ORDER BY date DESC;
        """
        df = pd.read_sql(query, conn, params=(adv, seven_days_ago))
        conn.close()

        if df.empty:
            raise HTTPException(status_code=404, detail="No history found for the given advertiser.")

        # Ensure that all values are serializable
        return df.to_dict(orient="records")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
