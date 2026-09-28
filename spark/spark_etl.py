import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# On importe nos fonctions existantes pour l'insertion PostgreSQL
from spark.etl_mongo_to_postgres import load_transformed_offres, transform_hellowork
from api.ingest import transform_offre

def process_partition(iterator):
    """
    Fonction exécutée par les workers Spark.
    Elle prend un lot d'offres Spark, les formate et les insère dans PostgreSQL.
    """
    transformed_jobs = []
    
    for row in iterator:
        job = row.asDict(recursive=True)
        source = job.get("source", "inconnu")
        
        if source == "france_travail":
            try:
                transformed_jobs.append(transform_offre(job))
            except Exception as e:
                import traceback
                traceback.print_exc()
                pass
        elif source == "hellowork":
            try:
                transformed_jobs.append(transform_hellowork(job))
            except Exception:
                pass
                
    if transformed_jobs:
        # Insertion en base via psycopg
        inserted = load_transformed_offres(transformed_jobs)
        print(f"✅ Partition Spark : {inserted} offres insérées dans PostgreSQL.")

def run_spark_etl():
    print("🚀 Démarrage de PySpark...")
    
    # Initialisation de Spark en mode local (Simple et propre)
    spark = SparkSession.builder \
        .appName("ETL_Emploi_Spark") \
        .master("local[*]") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/emploi_datalake.raw_jobs") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    print("📦 Lecture des données depuis MongoDB (Data Lake)...")
    
    # 1. LECTURE (Extract)
    # Spark charge les données MongoDB dans un DataFrame distribué
    df_raw = spark.read.format("mongo").load()
    
    total_docs = df_raw.count()
    print(f"📊 {total_docs} offres chargées dans le DataFrame Spark.")
    
    # 2. TRAITEMENT SPARK (Transform)
    # Exemple de traitement avec Spark : on filtre les offres qui n'ont pas de source
    df_filtered = df_raw.filter(col("source").isNotNull())
    
    # 3. CHARGEMENT DISTRIBUÉ (Load vers PostgreSQL)
    print("🔄 Début du traitement et chargement vers PostgreSQL via les workers Spark...")
    # On distribue l'insertion sur les partitions Spark
    df_filtered.rdd.foreachPartition(process_partition)
    
    print("✅ Pipeline PySpark terminé avec succès !")
    
    # Pause pour permettre de voir l'UI Spark (qui se ferme avec l'application)
    print("\n" + "="*60)
    print("🌐 L'interface locale Spark (Spark UI) est disponible !")
    print("👉 Ouvre ton navigateur et va sur : http://localhost:4040")
    print("="*60 + "\n")
    input("Appuie sur [ENTRÉE] pour fermer l'application Spark et quitter...")
    
    spark.stop()

if __name__ == "__main__":
    run_spark_etl()
