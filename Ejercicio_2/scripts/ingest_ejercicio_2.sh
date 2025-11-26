###############################################################################################
# Archivo: ingest_ejercicio_2.sh  Ubicacion: /home/hadoop/scripts
#
# Descarga e Ingest en HDFS de los archivos:
# https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv
# https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv
# 
###############################################################################################

# Punto 2 

# Descarga datos desde el repositorio al directorio /home/hadoop/landing
wget -P /home/hadoop/landing https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv
wget -O /home/hadoop/landing/georef_usa.csv https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv


# Lleva el archivo a HDFS al directorio /ingest
hdfs dfs -put /home/hadoop/landing/CarRentalData.csv /ingest
hdfs dfs -put /home/hadoop/landing/georef_usa.csv /ingest
 

# Borra el archivo starwars.csv del directorio /home/hadoop/landing/
rm /home/hadoop/landing/CarRentalData.csv
rm /home/hadoop/landing/georef_usa.csv
