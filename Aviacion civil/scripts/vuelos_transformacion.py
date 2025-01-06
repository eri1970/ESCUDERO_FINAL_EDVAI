# ESCUDERO FINAL ejercio 1 transformación funcionando V-01-01-2025
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import lit



# 0. Configuración de SparkSession con soporte para Hive
spark = SparkSession.builder \
    .appName("Airport Trips Processing") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. Leemos los archivos y creamos el DataFrames
df_2021 = spark.read.option("header", "true").option("sep", ";").csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv")
df_2022 = spark.read.option("header", "true").option("sep", ";").csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv")
df_aeroport = spark.read.option("header", "true").option("sep", ";").csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv")


# Eliminar columna "calidad_del_dato" de df_2021 y df_2022
df_2021 = df_2021.drop("calidad dato")
df_2022 = df_2022.drop("calidad dato")

# 2. Union df_2021 y df_2022

df_vuelos = df_2021.union(df_2022)


# Eliminar columnas "inhab" y "fir" de df_aeroport
df_aeroport = df_aeroport.drop("inhab", "fir")

# hay que renombrar algunas col de aeroport local >> aeropuerto y oaci >>>> aoc

df_aeroport = df_aeroport.withColumnRenamed("local", "aeropuerto").withColumnRenamed("oaci", "aoc")

# Filtra por vuelos domesticos, elimina columna 'calidad del dato' y reemplaza nulos de la columna 'pasajeros' por ceros

df_vuelos_mod = df_vuelos \
    .withColumn("Pasajeros", F.col("Pasajeros").cast("int")) \
    .withColumn("Fecha", F.to_date(df_vuelos["Fecha"], "dd/MM/yyyy").alias("Fecha")) \
    .replace({'Doméstico': 'Domestico'}, subset=['Clasificación Vuelo'])

vuelos_filtered = df_vuelos_mod \
    .filter(df_vuelos_mod['Clasificación Vuelo'] == "Domestico") \
    .fillna(0, 'pasajeros')


# Renombrar las columnas para que coincidan con las de la tabla de Hive
vuelos_filtered_renamed = vuelos_filtered \
    .withColumnRenamed("Fecha", "fecha") \
    .withColumnRenamed("Hora UTC", "hora_utc") \
    .withColumnRenamed("Clase de Vuelo (todos los vuelos)", "clase_de_vuelo") \
    .withColumnRenamed("Clasificación Vuelo", "clasificacion_de_vuelo") \
    .withColumnRenamed("Tipo de Movimiento", "tipo_de_movimiento") \
    .withColumnRenamed("Aeropuerto", "aeropuerto") \
    .withColumnRenamed("Origen / Destino", "origen_destino") \
    .withColumnRenamed("Aerolinea Nombre", "aerolinea_nombre") \
    .withColumnRenamed("Aeronave", "aeronave") \
    .withColumnRenamed("Pasajeros", "pasajeros")

# Inserta tabla "vuelos_filtered_reanmed" en la BD vuelosdb, en la tabla 'aeropuerto_tabla' funcionando 30-12

vuelos_filtered_renamed.write.insertInto("vuelosdb.aeropuerto_tabla", overwrite=True)
    
# reemplaza nulos de la columna 'distancia_ref' por ceros. 
df_aeroport = df_aeroport\
    .fillna(0, "distancia_ref")


# Convertir elev y distancia_ref a float
df_aeroport = df_aeroport.withColumn("elev", df_aeroport["elev"].cast(FloatType()))
df_aeroport = df_aeroport.withColumn("distancia_ref", df_aeroport["distancia_ref"].cast(FloatType()))

# reorganizacion de columnas

df_aeroport_adjusted = df_aeroport.withColumnRenamed("aoc", "oac")



# Seleccionar únicamente las columnas necesarias para la tabla Hive
columns_to_select = [
    "aeropuerto", "oac", "iata", "tipo", "denominacion", "coordenadas",
    "latitud", "longitud", "elev", "uom_elev", "ref", "distancia_ref",
    "direccion_ref", "condicion", "control", "region", "uso"
]
df_aeroport_filtered = df_aeroport_adjusted.select(columns_to_select)

# Agregar columnas faltantes con valores predeterminados
df_aeroport_adjusted = df_aeroport_filtered \
    .withColumn("trafico", lit(None)) \
    .withColumn("sna", lit(None)) \
    .withColumn("concesionado", lit(None)) \
    .withColumn("provincia", lit(None))

# Ordenamos y seleccionamo las columnas según la tabla de HIve    
column_order = [
    "aeropuerto", "oac", "iata", "tipo", "denominacion", "coordenadas",
    "latitud", "longitud", "elev", "uom_elev", "ref", "distancia_ref",
    "direccion_ref", "condicion", "control", "region", "uso", 
    "trafico", "sna", "concesionado", "provincia"
]
df_aeroport_adjusted = df_aeroport_adjusted.select(column_order)


#Insertamos df_aeroport en la DB vuelosdb tabla aeropurtos 

df_aeroport_adjusted.write.insertInto("vuelosdb.aeropuerto_detalles_tabla", overwrite=True)




        








