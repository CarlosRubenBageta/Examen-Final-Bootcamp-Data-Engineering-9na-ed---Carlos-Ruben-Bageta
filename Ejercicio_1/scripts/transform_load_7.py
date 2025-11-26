##################################################################################################################
# Archivo: transform_load_7.py
##################################################################################################################

# Importamos librerias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    # --- Parámetros de HDFS ---
    NN_URI = "hdfs://172.17.0.2:9000"  # ajusta si tu NameNode/puerto son otros
    INPUT_INFORME_1   = f"{NN_URI}/ingest/2021-informe-ministerio.csv"
    INPUT_INFORME_2   = f"{NN_URI}/ingest/202206-informe-ministerio.csv"
    INPUT_DETALLE_AEROPUERTOS = f"{NN_URI}/ingest/aeropuertos_detalle.csv"
    OUTPUT_TABLA_1_DW = f"{NN_URI}/tables/external/aeropuertos/aeropuerto_tabla"
    OUTPUT_TABLA_2_DW = f"{NN_URI}/tables/external/aeropuertos/aeropuerto_detalles_tabla"
    
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
    df = spark.read.csv(INPUT_INFORME_1, header=True, inferSchema=True, sep=";")
    df1 = spark.read.csv(INPUT_INFORME_2, header=True, inferSchema=True, sep=";")
    df2 = spark.read.csv(INPUT_DETALLE_AEROPUERTOS, header=True, inferSchema=True, sep=";")


    # 3) Transformaciones (casteo de tipos)

    # Casteo de df y df1 (ademas eliminamos la columna 'Calidad dato' que no es necesaria)

    def castear_vuelos(df): # Funcion para castear los dataframes de vuelos (sin 'Calidad dato')
        return df.select(
            F.to_date(F.col("Fecha"), "dd/MM/yyyy").alias("fecha"),
            F.col("Hora UTC").cast("string").alias("horaUTC"),
            F.col("Clase de Vuelo (todos los vuelos)").cast("string").alias("clase_de_vuelo"),
            F.col("Clasificación Vuelo").cast("string").alias("clasificacion_de_vuelo"),
            F.col("Tipo de Movimiento").cast("string").alias("tipo_de_movimiento"),
            F.col("Aeropuerto").cast("string").alias("aeropuerto"),
            F.col("Origen / Destino").cast("string").alias("origen_destino"),
            F.col("Aerolinea Nombre").cast("string").alias("aerolinea_nombre"),
            F.col("Aeronave").cast("string").alias("aeronave"),
            F.col("Pasajeros").cast("int").alias("pasajeros")
        )

    # Usamos la función para castear ambos dataframes

    df_cast = castear_vuelos(df)
    df1_cast = castear_vuelos(df1)

    # Unimos ambos dataframes casteados

    df_union = df_cast.unionByName(df1_cast)

    # Creamos una vista temporal union_v para hacer consultas SQL

    df_union.createOrReplaceTempView("union_v")

    # Filtramos solo los vuelos domésticos 

    df_domestico = spark.sql("""
        SELECT *
        FROM union_v
        WHERE lower(clasificacion_de_vuelo) IN ('doméstico', 'domestico')
        ORDER BY fecha DESC
    """)

    #  Rellenamos los valores nulos en la columna 'pasajeros' con 0

    df_load = df_domestico.fillna({"pasajeros": 0})

    # Casteo de df2 (aeropuertos detalle) No figura la columna 'inhab' ni 'fir' en la tabla destino, por lo que no se incluyen

    df2_cast = df2.select(
        # local -> aeropuerto
        F.col("local").cast("string").alias("aeropuerto"),
        F.col("oaci").cast("string").alias("oac"),
        F.col("iata").cast("string").alias("iata"),
        F.col("tipo").cast("string").alias("tipo"),
        F.col("denominacion").cast("string").alias("denominacion"),
        F.col("coordenadas").cast("string").alias("coordenadas"),
        F.col("latitud").cast("string").alias("latitud"),
        F.col("longitud").cast("string").alias("longitud"),
        F.col("elev").cast("float").alias("elev"),
        F.col("uom_elev").cast("string").alias("uom_elev"),
        F.col("ref").cast("string").alias("ref"),
        F.col("distancia_ref").cast("float").alias("distancia_ref"),
        F.col("direccion_ref").cast("string").alias("direccion_ref"),
        F.col("condicion").cast("string").alias("condicion"),
        F.col("control").cast("string").alias("control"),
        F.col("region").cast("string").alias("region"),
        F.col("uso").cast("string").alias("uso"),
        F.col("trafico").cast("string").alias("trafico"),
        F.col("sna").cast("string").alias("sna"),
        F.col("concesionado").cast("string").alias("concesionado"),
        F.col("provincia").cast("string").alias("provincia"),

    )

    #  Rellenamos los valores nulos en la columna 'distancia_ref' con 0

    df2_load = df2_cast.fillna({"distancia_ref": 0})


    # (opcional) métricas rápidas al log
    print(f"[INFO] filas vuelos        : {df_load.count()}")
    print(f"[INFO] filas aeropuertos detalles : {df2_load.count()}")
   
    # 5) Load (Parquet a HDFS)
    df_load.write.mode("overwrite").parquet(OUTPUT_TABLA_1_DW)
    df2_load.write.mode("overwrite").parquet(OUTPUT_TABLA_2_DW)
    
    spark.stop()

if __name__ == "__main__":
    main()