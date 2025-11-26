##################################################################################################################
# Archivo: transform_load_8.py
##################################################################################################################

# Importamos librerias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # --- Parámetros de HDFS ---
    NN_URI = "hdfs://172.17.0.2:9000"  # ajusta si tu NameNode/puerto son otros
    INPUT_1   = f"{NN_URI}/ingest/CarRentalData.csv"
    INPUT_2   = f"{NN_URI}/ingest/georef_usa.csv"
    OUTPUT_TABLA_1_DW = f"{NN_URI}/tables/external/car_rental_db/car_rental_analytics"

    
    # 1) Crear la sesión de Spark con soporte Hive y FS por defecto en HDFS
    spark = (
        SparkSession
            .builder
            .appName("transform_load_7")
            .enableHiveSupport()
            .config("spark.hadoop.fs.defaultFS", NN_URI)
            .getOrCreate()
    )

    # 2) Rutas de entrada (csv desde HDFS)
    df1 = spark.read.csv(INPUT_1, header=True, inferSchema=True, sep=",")
    df2 = spark.read.csv(INPUT_2, header=True, inferSchema=True, sep=";")

    # 3) Transformaciones (casteo de tipos)

    # Casteo de df1 y df2 (solo las columnas que nos hacen falta de este ultimo)

    df1_cast = df1.select(
        F.col("fuelType").cast("string").alias("fueltype"),
        F.round(F.col("rating"), 0).cast("int").alias("rating"), # Redondeamos el rating y casteamos a int
        F.col("renterTripsTaken").cast("int").alias("rentertripstaken"),
        F.col("reviewCount").cast("int").alias("reviewcount"),
        F.col("`location.city`").cast("string").alias("city"),
        F.col("`location.state`").cast("string").alias("state_id1"),
        F.col("`owner.id`").cast("int").alias("owner_id"),
        F.col("`rate.daily`").cast("int").alias("rate_daily"),
        F.col("`vehicle.make`").cast("string").alias("make"),
        F.col("`vehicle.model`").cast("string").alias("model"),
        F.col("`vehicle.year`").cast("int").alias("year")
    )
    df2_cast = df2.select(
        F.col("United States Postal Service state abbreviation").cast("string").alias("state_id2"), #sirve de columna para joinear con df1
        F.col("Official Name State").cast("string").alias("state_name")
        )

    # hacemos el join de ambos dataframes 

    dfjoin = df1_cast.join(df2_cast, df1_cast.state_id1 == df2_cast.state_id2, "left")

    # Reordenamos las columnas en el dataframe, para que queden alineadas con la tabla de Hive

    df = dfjoin.select(
        dfjoin.fueltype, 
        dfjoin.rating, 
        dfjoin.rentertripstaken, 
        dfjoin.reviewcount, 
        dfjoin.city, 
        dfjoin.state_name, 
        dfjoin.owner_id, 
        dfjoin.rate_daily, 
        dfjoin.make, 
        dfjoin.model, 
        dfjoin.year
        )

    # Creamos una vista temporal df_v para hacer consultas SQL

    df.createOrReplaceTempView("df_v")

    # Filtramos Texas y eliminamos registros nulos en la columna 'rating'

    df_clean = spark.sql("""
        SELECT *
        FROM df_v
        WHERE state_name <> 'Texas' AND rating IS NOT NULL
    """)
   
    # 4) Load (Parquet a HDFS)
    df_clean.write.mode("overwrite").parquet(OUTPUT_TABLA_1_DW)
    
    spark.stop()

if __name__ == "__main__":
    main()