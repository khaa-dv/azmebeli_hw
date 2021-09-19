from config import URL_HOST, POSTGRES_PORT
import os

os.system('docker pull postgres')
os.system('docker run --name postgres_az -p '+POSTGRES_PORT+':5432 -e POSTGRES_PASSWORD=postgres -d postgres:latest')
