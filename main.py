from fastapi import FastAPI, HTTPException
from psycopg2 import connect, sql
from psycopg2.extras import RealDictCursor
import logging
from typing import List, Dict

# Configuración de la  appi
app = FastAPI(title="Sistema de Recomendaciones Publicitarias")
logging.basicConfig(level=logging.INFO)

# Configuración de la base de datos
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "nicosofivlad",
    "host": "grupo-18-rds2.cf4i6e6cwv74.us-east-1.rds.amazonaws.com",
    "port": "5432",
}

# Función de conexión a la base de datos
def get_db_connection():
    try:
        conn = connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logging.error(f"Error al conectar con la base de datos: {e}")
        raise HTTPException(status_code=500, detail="Error al conectar con la base de datos")

@app.get("/")
def home():
    return {"message": "API de Recomendaciones Publicitarias está en funcionamiento"}

# Endpoint: /recommendations/<ADV>/<Modelo>
@app.get("/recommendations/{adv}/{model}")
def get_recommendations(adv: str, model: str) -> List[Dict]:
    """Devuelve las recomendaciones para un advertiser y modelo específico."""
    table_name = "top_ctr" if model == "TopCTR" else "top_products" if model == "TopProduct" else None
    if not table_name:
        raise HTTPException(status_code=400, detail="Modelo inválido. Use 'TopCTR' o 'TopProduct'.")
    
    query = sql.SQL("SELECT * FROM {} WHERE advertiser_id = %s").format(sql.Identifier(table_name))
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (adv,))
            recommendations = cursor.fetchall()
        conn.close()
        return recommendations
    except Exception as e:
        logging.error(f"Error al obtener recomendaciones: {e}")
        raise HTTPException(status_code=500, detail="Error al obtener recomendaciones")

# Endpoint: /stats/
@app.get("/stats/")
def get_stats() -> Dict:
    """Devuelve estadísticas de los datos almacenados."""
    stats = {
        "cantidad_anunciantes": 0,
        "mayor_variacion_recomendaciones": None,
    }
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Cantidad de advertisers
            cursor.execute("SELECT COUNT(DISTINCT advertiser_id) FROM top_ctr")
            stats["cantidad_anunciantes"] = cursor.fetchone()[0]
            
            # (Ejemplo: podrías agregar lógica para calcular la mayor variación aquí)
        conn.close()
        return stats
    except Exception as e:
        logging.error(f"Error al obtener estadísticas: {e}")
        raise HTTPException(status_code=500, detail="Error al obtener estadísticas")

# Endpoint: /history/<ADV>
@app.get("/history/{adv}")
def get_history(adv: str) -> List[Dict]:
    """Devuelve el historial de recomendaciones de los últimos 7 días para un advertiser."""
    query = """
        SELECT * FROM (
            SELECT * FROM top_ctr WHERE advertiser_id = %s
            UNION
            SELECT * FROM top_products WHERE advertiser_id = %s
        ) AS combined
        WHERE date >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY date DESC
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (adv, adv))
            history = cursor.fetchall()
        conn.close()
        return history
    except Exception as e:
        logging.error(f"Error al obtener historial: {e}")
        raise HTTPException(status_code=500, detail="Error al obtener historial")

