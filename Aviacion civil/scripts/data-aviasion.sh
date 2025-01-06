#Escudero final ejercicio 1 -Aviación Civil-
#!/bin/bash

# Mensaje de inicio
echo "****** Inicio Ingesta Aviacion Civil ******"

# 1️ Crear el directorio 'landing' en /home/hadoop
mkdir -p /home/hadoop/landing 

# 2️ Eliminar todos los archivos en el directorio local 'landing'
rm -f /home/hadoop/landing/*

# 3️ Descargar los archivos desde las URLs especificadas

wget -P /home/hadoop/landing "https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv"

wget -P /home/hadoop/landing "https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv"

wget -P /home/hadoop/landing "https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv"

# 4️ Eliminar todos los archivos en el directorio HDFS '/ingest'
hdfs dfs -rm -f /ingest/*

# 5️ Subir los nuevos archivos desde 'landing' a HDFS '/ingest' 
hdfs dfs -put /home/hadoop/landing/* /ingest/

# Mensaje de finalizacion
echo "\n****** Fin Ingesta Aviacion Civil ******"

