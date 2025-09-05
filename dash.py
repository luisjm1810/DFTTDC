import psycopg2
import pandas as pd
import streamlit as st

# Datos de conexión
DB_HOST = "192.168.100.13"
DB_NAME = "Sincro"
DB_USER = "postgres"
DB_PASS = "$c27d19a11"

# Consulta SQL
query = """
WITH fechas AS (
  SELECT 
     MAX(TO_DATE("Fecha_DFT", 'MMDDYY')) AS fecha_max
  FROM "Sincro"."DFTTDC"
),
ayer AS (
  SELECT DISTINCT TRIM("CC_Suc") AS "CC_Suc"
  FROM "Sincro"."DFTTDC", fechas
  WHERE TO_DATE("Fecha_DFT", 'MMDDYY') = fecha_max - INTERVAL '1 day'
),
hoy AS (
  SELECT DISTINCT TRIM("CC_Suc") AS "CC_Suc"
  FROM "Sincro"."DFTTDC", fechas
  WHERE TO_DATE("Fecha_DFT", 'MMDDYY') = fecha_max
)

SELECT 
  COALESCE(ayer."CC_Suc", hoy."CC_Suc") AS "CC_Suc",
  CASE WHEN ayer."CC_Suc" IS NOT NULL THEN '✔' ELSE '✘' END AS "Ayer",
  CASE WHEN hoy."CC_Suc" IS NOT NULL THEN '✔' ELSE '✘' END AS "Hoy"
FROM ayer 
FULL OUTER JOIN hoy ON ayer."CC_Suc" = hoy."CC_Suc"
ORDER BY "CC_Suc";
"""

# Función de conexión y ejecución
def run_query(query):
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# Interfaz en Streamlit
st.title("📊 Comparativo Ayer vs Hoy - Sucursales")
st.write("Consulta de la tabla **Sincro.DFTTDC** en PostgreSQL")

try:
    data = run_query(query)
    st.dataframe(data)
except Exception as e:
    st.error(f"Error al ejecutar la consulta: {e}")
