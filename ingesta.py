import boto3
import pymysql
import csv

# Configuración
ficheroUpload = "data.csv"
nombreBucket = "lucechaos-storage"

# Conexión MySQL
conexion = pymysql.connect(
    host="host_mysql",
    user="usuario",
    password="password",
    database="basedatos"
)
cursor = conexion.cursor()
cursor.execute("SELECT * FROM mitabla")
rows = cursor.fetchall()

# Guardar CSV
with open(ficheroUpload, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([col[0] for col in cursor.description])
    writer.writerows(rows)

cursor.close()
conexion.close()

# Subir a S3
s3 = boto3.client('s3')
s3.upload_file(ficheroUpload, nombreBucket, "ingesta/" + ficheroUpload)

print("Ingesta completada")
