####################################################################################
# Archivo: ingest_ejercicio_1.sh
#
# Descarga e Ingest en HDFS de los archivos:
# https://data-engineer-edvai-public.s3.amazonaws.com/2021-informe-ministerio.csv
# https://data-engineer-edvai-public.s3.amazonaws.com/202206-informe-ministerio.csv
# https://data-engineer-edvai-public.s3.amazonaws.com/aeropuertos_detalle.csv 
####################################################################################

# Punto 1 

# Descarga datos desde el repositorio al directorio /home/hadoop/landing
wget -P /home/hadoop/landing https://data-engineer-edvai-public.s3.amazonaws.com/2021-informe-ministerio.csv
wget -P /home/hadoop/landing https://data-engineer-edvai-public.s3.amazonaws.com/202206-informe-ministerio.csv
wget -P /home/hadoop/landing https://data-engineer-edvai-public.s3.amazonaws.com/aeropuertos_detalle.csv


# Lleva el archivo a HDFS al directorio /ingest
hdfs dfs -put /home/hadoop/landing/2021-informe-ministerio.csv /ingest
hdfs dfs -put /home/hadoop/landing/202206-informe-ministerio.csv /ingest
hdfs dfs -put /home/hadoop/landing/aeropuertos_detalle.csv /ingest
 

# Borra el archivo starwars.csv del directorio /home/hadoop/landing/
rm /home/hadoop/landing/2021-informe-ministerio.csv
rm /home/hadoop/landing/202206-informe-ministerio.csv
rm /home/hadoop/landing/aeropuertos_detalle.csv
