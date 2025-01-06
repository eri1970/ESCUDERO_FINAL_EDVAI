#!/bin/bash

# Mensaje de inicio 
echo "****** Inicio Ingesta Rent a Car ******"

# Directorio landing en hadoop
LANDING_DIR="/home/hadoop/landing"

# Directorio destino en HDFS
DEST_DIR="/ingest2"

# Nombre archivos
rentacar="CarRentalData.csv"
georef="georef-united-states-of-america-state.csv"

# Descarga archivos
wget -P $LANDING_DIR -O $LANDING_DIR/$rentacar "https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv"
wget -P $LANDING_DIR -O $LANDING_DIR/$georef "https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv"

# Mover archivos a HDFS
hdfs dfs -put $LANDING_DIR/$rentacar $DEST_DIR
hdfs dfs -put $LANDING_DIR/$georef $DEST_DIR

# Remueve archivos, asegurando que el archivo existe
rm -f $LANDING_DIR/$rentacar
rm -f $LANDING_DIR/$georef

# Mensaje de finalización
echo "\n****** Fin Ingesta rent a car ******"
