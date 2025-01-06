# ESCUDERO FINAL ejercio 2 transformación funcionando V-01-01-2025
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import lit
from pyspark.sql.functions import round, col
from pyspark.sql.functions import col
from pyspark.sql import functions as F

# 0. Configuración de SparkSession con soporte para Hive
spark = SparkSession.builder \
    .appName("Airport Trips Processing") \
    .enableHiveSupport() \
    .getOrCreate()
    
    
# 1.- leemos los datos y creamos los DataFrames
df_rentacar = spark.read.option("header", "true").option("sep", ",").csv("hdfs://172.17.0.2:9000/ingest/CarRentalData.csv")
#df_georef = spark.read.option("header", "true").option("sep", ";").csv("hdfs://172.17.0.2:9000/ingest/georef-united-states-of-america-state.csv")

# 1.1- Limpieza del segundo set de datos en Collab, ingestamos un nuevo csv ya listo para las transformaciones
df_georef2 = spark.read.option("header", "true").option("sep", ",").csv("hdfs://172.17.0.2:9000/ingest/us_state_clean.csv")

# 2 Redondear la columna 'rating' y castear a int
df_rentacar = df_rentacar.withColumn("rating", round(col("rating")).cast("int"))

# Mostrar el resultado
df_rentacar.show(5, truncate=False)

# vamos a hacer un mapping entre los codigo y las cuidades para unir los dos DF

# Crear el diccionario de mapeo basado en el archivo
# Seleccionar las columnas Code_State y US_Postal_state
df_georef2_mapping = df_georef2.select("Code_State", "US_Postal_state").distinct()

# Convertir el DataFrame a un RDD y colectar los resultados
mapping_list = df_georef2_mapping.rdd.collect()

# Crear el diccionario de mapeo
state_mapping = {str(row["Code_State"]): row["US_Postal_state"] for row in mapping_list}

# Ver el diccionario
state_mapping

#agregamos la columna 'state_code' al df_georef2
df_georef2 = df_georef2.withColumn('state_code', F.lit(None))

# Realizar el inner join entre df_rentacar y df_georef2 usando las columnas correspondientes
renta_join = df_rentacar.join(df_georef2, df_rentacar['`location.state`'] == df_georef2['state_code'], 'inner')

renta_join.show(10, truncate=False)

#se genero el nuevo DF unido

# Eliminar los registros con rating nulo
renta_join_cleaned = renta_join.filter(col("rating").isNotNull())

# Cambiar mayúsculas por minúsculas en la columna 'fuelType'
renta_join_cleaned = renta_join_cleaned.withColumn('fuelType', col('fuelType').lower())

# Excluir el estado Texas (TX)
renta_join_cleaned = renta_join_cleaned.filter(col('`location.state`') != 'TX')

# Mostrar el resultado después de las operaciones
renta_join_cleaned.show(100, truncate=False)

#vamos a prepara el DF para la insercion en HIVE

# primero sacamos las columnas que no vamos a utilizar en HIVE

df_rental_hive = renta_join_cleaned.drop('Geo_Point', 'Year', 'Code_State', 'Name_State', 'iso_area_code', 'Type','US_Postal_state', 'State_FIPS_Code', 'State_GNIS_Code', 'state_code')

#renombramos y casteamos las columnas y datos

df_rental_hive = df_rental_hive.selectExpr(
    "fuelType as fueltype",
    "CAST(rating AS INT) as rating",
    "CAST(renterTripsTaken AS INT) as rentertripstaken",
    "CAST(reviewCount AS INT) as reviewcount",
    "`location.city` as city",  # Usar backticks para acceder a columnas con puntos en el nombre
    "`location.state` as state_name",  # Usar backticks para acceder a columnas con puntos en el nombre
    "CAST(`rate.daily` AS INT) as rate_daily",  # Usar backticks para rate.daily
    "`vehicle.make` as make",  # Usar backticks para vehicle.make
    "`vehicle.model` as model",  # Usar backticks para vehicle.model
    "CAST(`vehicle.year` AS INT) as year"  # Usar backticks para vehicle.year
)
# insertamos en hive

df_rental_hive.write.saveAsTable('car_rental_analytics', mode='overwrite')