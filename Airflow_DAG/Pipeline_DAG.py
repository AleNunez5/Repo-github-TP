import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3
from io import StringIO
import psycopg2


def filter_products_and_ads(execution_date,bucket_name, advertiser_file_name, products_file_name, ads_file_name,access_key,secret_access):
    # Obtener la fecha anterior de ejecución del scheduler
    fecha = execution_date - timedelta(days=1)
    fecha = datetime(fecha.year, fecha.month, fecha.day)
    
    # Leer el archivo de IDs de anunciantes desde S3
    s3 = boto3.client('s3',
                      region_name='us-east-2',
                      aws_access_key_id= access_key,
                      aws_secret_access_key= secret_access)
    obj = s3.get_object(Bucket=bucket_name, Key=advertiser_file_name)
    advertisers_df = pd.read_csv(obj['Body'])
    
    # Filtrar el archivo de productos por la fecha y los IDs de anunciantes activos
    obj = s3.get_object(Bucket=bucket_name, Key=products_file_name)
    products_df = pd.read_csv(obj['Body'], parse_dates=['date'])
    filtered_products_df = products_df[(products_df['date'] == fecha) & 
                                       (products_df['advertiser_id'].isin(advertisers_df['advertiser_id']))]
    
    # Guardar el DataFrame filtrado de productos como un archivo CSV en S3
    csv_buffer = StringIO()
    filtered_products_df.to_csv(csv_buffer, index=False, date_format='%Y-%m-%d')
    s3_resource = boto3.resource('s3',
                                 region_name='us-east-2',
                                 aws_access_key_id=access_key,
                                 aws_secret_access_key=secret_access)
    s3_resource.Object(bucket_name, f'filter_products_views_{fecha}.csv').put(Body=csv_buffer.getvalue())
    print(f"DataFrame de productos filtrado guardado como filter_products_views_{fecha}.csv en el bucket S3.")

    # Filtrar el archivo de anuncios por la fecha y los IDs de anunciantes activos
    obj = s3.get_object(Bucket=bucket_name, Key=ads_file_name)
    ads_df = pd.read_csv(obj['Body'], parse_dates=['date'])
    filtered_ads_df = ads_df[(ads_df['date'] == fecha) & 
                             (ads_df['advertiser_id'].isin(advertisers_df['advertiser_id']))]
    
    # Guardar el DataFrame filtrado de anuncios como un archivo CSV en S3
    csv_buffer = StringIO()
    filtered_ads_df.to_csv(csv_buffer, index=False, date_format='%Y-%m-%d')
    s3_resource.Object(bucket_name, f'filter_ads_views_{fecha}.csv').put(Body=csv_buffer.getvalue())
    print(f"DataFrame de anuncios filtrado guardado como filter_ads_views_{fecha}.csv en el bucket S3.")


def compute_top_products(execution_date,bucket_name, access_key, secret_access):
    
    # Obtener la fecha anterior de ejecución del scheduler
    fecha = execution_date - timedelta(days=1)
    fecha = datetime(fecha.year, fecha.month, fecha.day)
    
    # Leer el archivo de productos filtrados desde S3
    s3 = boto3.client('s3',
                      region_name='us-east-2',
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_access)
    obj = s3.get_object(Bucket=bucket_name, Key=f'filter_products_views_{fecha}.csv')
    filtered_products_df = pd.read_csv(obj['Body'])

    # Computar los productos más vistos por cada anunciante (advertiser)
    top_products_df = filtered_products_df.groupby(['advertiser_id', 'product_id', 'date'])['date'].count().reset_index(name='views') \
                                          .groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'views')).reset_index(drop=True)

    # Guardar el DataFrame de los productos más vistos como un archivo CSV en S3
    csv_buffer = StringIO()
    top_products_df.to_csv(csv_buffer, index=False, date_format='%Y-%m-%d')
    s3_resource = boto3.resource('s3',
                                 region_name='us-east-2',
                                 aws_access_key_id=access_key,
                                 aws_secret_access_key=secret_access)
    s3_resource.Object(bucket_name,f'top_products_{fecha}.csv').put(Body=csv_buffer.getvalue())


def compute_top_ctr(execution_date,bucket_name, access_key, secret_access):
    
    # Obtener la fecha anterior de ejecución del scheduler
    fecha = execution_date - timedelta(days=1)
    fecha = datetime(fecha.year, fecha.month, fecha.day)
    
    # Leer el archivo de anuncios filtrados desde S3
    s3 = boto3.client('s3',
                      region_name='us-east-2',
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_access)
    obj = s3.get_object(Bucket=bucket_name, Key=f'filter_ads_views_{fecha}.csv')
    ads_df = pd.read_csv(obj['Body'])
    print(ads_df.head())  # Verificar los primeros registros del DataFrame después de leer el archivo CSV
    # Calcular click-through-rate (CTR) por producto, anunciante y fecha
    ctr_df = ads_df.groupby(['advertiser_id', 'product_id', 'date']).agg(
        clicks=('type', lambda x: (x == 'click').sum()),
        impressions=('type', 'count')  # Suma de clicks e impresiones
        ).reset_index()
    
    print(ctr_df.head())  # Verificar los primeros registros del DataFrame después de la operación groupby
    # Calcular CTR, manejar el caso donde el denominador es cero
    ctr_df['CTR'] = ctr_df['clicks'] / ctr_df['impressions'] 

    # Ordenar los productos según su CTR y quedarnos con los 20 mejores para cada anunciante
    top_ctr_df = ctr_df.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'CTR')).reset_index(drop=True)

    # Agregar una nueva columna 'type' con el valor 'CTR'
    top_ctr_df['type'] = 'CTR'
    print(ctr_df.head())
    # Guardar los resultados en un nuevo archivo CSV en S3
    csv_buffer = StringIO()
    top_ctr_df.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3',
                                 region_name='us-east-2',
                                 aws_access_key_id=access_key,
                                 aws_secret_access_key=secret_access)
    s3_resource.Object(bucket_name, f'top_ctr_{fecha}.csv').put(Body=csv_buffer.getvalue())



def db_writting(execution_date,bucket_name, secret_access):
    try:
        
        # Obtener la fecha anterior de ejecución del scheduler
        fecha = execution_date - timedelta(days=1)     
        fecha = datetime(fecha.year, fecha.month, fecha.day)
        
        # Conexión a la base de datos RDS
        conn = psycopg2.connect(
            database="",
            user="postgres",
            password="DBudesa2024",
            host="db-tp.clsgko4kiy7p.us-east-2.rds.amazonaws.com",
            port='5432'
        )
        cursor = conn.cursor()

        # Crear las tablas si no existen
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TOP_CTR_Recomendaciones (
                advertiser_id VARCHAR(255),
                product_id VARCHAR(255),
                date DATE,
                clicks INT,
                impressions INT,
                CTR FLOAT,
                type VARCHAR(255),
                PRIMARY KEY (advertiser_id,product_id,date)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TOP_Products_Recomendaciones (
                advertiser_id VARCHAR(255),
                product_id VARCHAR(255),
                date DATE,
                views INT,
                PRIMARY KEY (advertiser_id,product_id,date)
            )
        """)

        # Leer el archivo CSV top_ctr_{date}.csv desde S3
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access)
        obj_ctr = s3.get_object(Bucket=bucket_name, Key=f'top_ctr_{fecha}.csv')
        ctr_data = obj_ctr['Body'].read().decode('utf-8')

        # Leer el archivo CSV top_products_{date}.csv desde S3
        obj_products = s3.get_object(Bucket=bucket_name, Key=f'top_products_{fecha}.csv')
        products_data = obj_products['Body'].read().decode('utf-8')

        # Convertir los datos a DataFrame
        ctr_df = pd.read_csv(StringIO(ctr_data))
        products_df = pd.read_csv(StringIO(products_data))

        # Asegurarse de que el campo 'date' sea de tipo datetime
        ctr_df['date'] = pd.to_datetime(ctr_df['date'])
        products_df['date'] = pd.to_datetime(products_df['date'])

        # Insertar los datos en la tabla TOP_CTR_Recomendaciones
        for index, row in ctr_df.iterrows():
            cursor.execute("""
                INSERT INTO TOP_CTR_Recomendaciones (advertiser_id, product_id, date, clicks, impressions, CTR, type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row['advertiser_id'], row['product_id'], row['date'], row['clicks'], row['impressions'], row['CTR'], row['type']))

        # Insertar los datos en la tabla TOP_Products_Recomendaciones
        for index, row in products_df.iterrows():
            cursor.execute("""
                INSERT INTO TOP_Products_Recomendaciones (advertiser_id, product_id, date, views)
                VALUES (%s, %s, %s, %s)
            """, (row['advertiser_id'], row['product_id'], row['date'], row['views']))
        
        # Confirmar los cambios
        conn.commit()

    except Exception as e:
        print("Error al escribir en la base de datos:", e)

    finally:
        # Cerrar cursor y conexión
        cursor.close()
        conn.close()


# Configura los detalles del bucket y los nombres de los archivos
bucket_name = 'bucket-tp-s3'
advertiser_file_name = 'advertiser_ids.csv'
products_file_name = 'product_views.csv'
ads_file_name = 'ads_views.csv'
"access_key="""
"secret_access="""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),}


# Define el DAG
with DAG(
    dag_id='Pipeline_DAG',
    schedule_interval="@daily",
    start_date=datetime(2024, 4,29),
    default_args=default_args,
    catchup=True
) as dag:

    # Define la 1 tarea
    filter_products_and_ads_task = PythonOperator(
        task_id='filter_products_and_ads',
        python_callable=filter_products_and_ads,
        op_kwargs={'bucket_name': bucket_name, 
                   'advertiser_file_name': advertiser_file_name, 
                   'products_file_name': products_file_name,
                   'ads_file_name': ads_file_name,
                   'access_key': access_key,
                   'secret_access': secret_access},
        provide_context=True)
    
    #Define la segunda tarea
    top_product_task = PythonOperator(
        task_id='compute_top_products',
        python_callable=compute_top_products,
        op_kwargs={'bucket_name': bucket_name,
                   'access_key': access_key,
                   'secret_access': secret_access},
        provide_context=True)
    
    # Define la tercera tarea
    top_ctr_task = PythonOperator(
        task_id='compute_top_ctr',
        python_callable=compute_top_ctr,
        op_kwargs={'bucket_name': bucket_name,
                   'access_key': access_key,
                   'secret_access': secret_access},
        provide_context=True)
    
    # Define la cuarta tarea
    db_writting_task = PythonOperator(
        task_id='db_writting',
        python_callable=db_writting,
        op_kwargs={
            'bucket_name': bucket_name,
            'access_key': access_key,
            'secret_access': secret_access},
        provide_context=True)
    
    # Define las dependencias 
    filter_products_and_ads_task >> top_product_task
    filter_products_and_ads_task >> top_ctr_task
    [top_product_task, top_ctr_task] >> db_writting_task
