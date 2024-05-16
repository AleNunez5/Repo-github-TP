from fastapi import FastAPI, HTTPException
import psycopg2
from typing import List, Dict
from datetime import datetime, timedelta

# Configuración de la conexión a la base de datos RDS
conn = psycopg2.connect(
    database="",
    user="postgres",
    password="DBudesa2024",
    host="db-tp.clsgko4kiy7p.us-east-2.rds.amazonaws.com",
    port='5432')

app = FastAPI()

def format_recommendations_1(recommendations: List[Dict[str, str]]) -> Dict[str, List[str]]:
    formatted_recommendations = {}
    for rec in recommendations:
        user_id = rec[0]  # asumo que el ID del usuario es el primer elemento de cada registro
        product_id = rec[1]  # asumo que el ID del producto es el segundo elemento de cada registro
        formatted_recommendations.setdefault(user_id, []).append(product_id)
    return formatted_recommendations

@app.get("/recommendations/{adv}/{modelo}")
def get_recommendations(adv: str, modelo: str):
    if modelo.lower() not in {"top_products", "top_ctr"}:
        raise HTTPException(status_code=404, detail="Modelo no encontrado")
    
    table_name = f"{modelo.upper()}_Recomendaciones"

    cur = conn.cursor()
    query = f"SELECT * FROM {table_name} WHERE advertiser_id = '{adv.upper()}' AND date = CURRENT_DATE - interval '1 day'"
    cur.execute(query)
    recommendations = cur.fetchall()
    cur.close()

    if not recommendations:
        raise HTTPException(status_code=404, detail="El anunciante no se encuentra activo")

    formatted_recommendations = format_recommendations_1(recommendations)
    return {f"{modelo.upper()}_Recomendaciones": formatted_recommendations}



def format_recommendations_2(recommendations: List[Dict[str, str]]) -> Dict[str, Dict[str, List[str]]]:
    formatted_recommendations = {}
    for rec in recommendations:
        user_id = rec[0]  # Supongo que el ID del usuario es el primer elemento de cada registro
        date = rec[1]  # Supongo que la fecha es el segundo elemento de cada registro
        product_id = rec[2]  # Supongo que el ID del producto es el tercer elemento de cada registro
        
        # Agrupar las recomendaciones por fecha 
        formatted_recommendations.setdefault(date, {}).setdefault(user_id, []).append(product_id)
    return formatted_recommendations


@app.get("/history/{adv}/{modelo}")
def get_history(adv: str, modelo: str):
    if modelo.lower() not in {"top_products", "top_ctr"}:
        raise HTTPException(status_code=404, detail="Modelo no encontrado")
    
    table_name = f"{modelo.upper()}_Recomendaciones"

    end_date = datetime.now().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=6)

    cur = conn.cursor()
    query = f"SELECT advertiser_id, date, product_id FROM {table_name} WHERE advertiser_id = '{adv.upper()}' AND date >= %s AND date <= %s"
    cur.execute(query, (start_date, end_date))
    recommendations = cur.fetchall()
    cur.close()

    if not recommendations:
        raise HTTPException(status_code=404, detail="El anunciante no se encuentra activo")

    formatted_recommendations = format_recommendations_2(recommendations)
    
    # Ordenar las fechas
    sorted_recommendations = dict(sorted(formatted_recommendations.items()))    
    
    return {"modelo":f"{modelo.upper()}","advertiser_id": adv, "recommendations": sorted_recommendations}



def calculate_stats() -> Dict[str, Dict[str, List[str]]]:
    stats = {}
    cur = conn.cursor()

    # Obtener los datos de CTR
    cur.execute("SELECT advertiser_id, product_id, \
                 CAST(ROUND(AVG(CTR) * 100) / 100 AS NUMERIC(10,2)) AS avg_ctr \
                 FROM TOP_CTR_Recomendaciones \
                 WHERE date >= '2024-04-29' AND date <= CURRENT_DATE - interval '1 day' \
                 GROUP BY advertiser_id, product_id")
    ctr_rows = cur.fetchall()

    # Obtener los datos de vistas
    cur.execute("SELECT advertiser_id, product_id, \
                 CAST(ROUND(AVG(views) * 100) / 100 AS NUMERIC(10,2)) AS avg_views \
                 FROM TOP_Products_Recomendaciones \
                 WHERE date >= '2024-04-29' AND date <= CURRENT_DATE - interval '1 day' \
                 GROUP BY advertiser_id, product_id")
    views_rows = cur.fetchall()

    cur.close()

    # Calcular el CTR promedio y las vistas promedio para cada producto por anunciante
    for row in ctr_rows:
        advertiser_id, product_id, avg_ctr = row
        stats.setdefault(advertiser_id, {"CTR promedio": [], "Vistas promedio": []})
        stats[advertiser_id]["CTR promedio"].append({product_id: avg_ctr})

    for row in views_rows:
        advertiser_id, product_id, avg_views = row
        stats.setdefault(advertiser_id, {"CTR promedio": [], "Vistas promedio": []})
        stats[advertiser_id]["Vistas promedio"].append({product_id: avg_views})

    # Ordenar los productos por CTR promedio y vistas promedio y seleccionar los 5 mejores
    for advertiser_id, products in stats.items():
        ctr_sorted = sorted(products["CTR promedio"], key=lambda x: list(x.values())[0], reverse=True)[:5]
        views_sorted = sorted(products["Vistas promedio"], key=lambda x: list(x.values())[0], reverse=True)[:5]
        stats[advertiser_id]["CTR promedio"] = ctr_sorted
        stats[advertiser_id]["Vistas promedio"] = views_sorted

    return stats

@app.get("/stats/")
def get_stats():
    stats = calculate_stats()
    return {"TOP 5 products con mayor TCR y Vistas Promedio por Advertiser": stats}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
