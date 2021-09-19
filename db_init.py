from config import URL_HOST, POSTGRES_PORT, USER_DB, PSWD_DB

# создание базы и пользователя
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine

con = psycopg2.connect(dbname='postgres', user='postgres', host=URL_HOST, port=POSTGRES_PORT, password='postgres')
con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = con.cursor()
cur.execute(sql.SQL("CREATE USER "+USER_DB+" WITH ENCRYPTED PASSWORD \'"+PSWD_DB+"\';"))
cur.execute(sql.SQL("CREATE DATABASE db_mebel;"))
cur.execute(sql.SQL("GRANT ALL PRIVILEGES ON DATABASE db_mebel TO "+USER_DB+";"))
cur.close()

alchemyEngine = create_engine('postgresql+psycopg2://'+USER_DB+':'+PSWD_DB+'@'+URL_HOST+':'+POSTGRES_PORT+'/db_mebel', pool_recycle=3600);

# схема для справочников
alchemyEngine.execute("CREATE SCHEMA hdbk")

# схема для основной таблицы
alchemyEngine.execute("CREATE SCHEMA product")

# основная таблица
alchemyEngine.execute("CREATE TABLE product.t_divan (id_divan SERIAL PRIMARY KEY, id_divan_from_source int, id_artikul int, id_in_stock int, price float, price_discount float, id_pricecurrency int, date_snap timestamp)")

# справочник артикулов
alchemyEngine.execute("CREATE TABLE hdbk.d_artikul (id_artikul SERIAL PRIMARY KEY, num_artikul int, name_divan varchar)")

# справочник значений в наличии, под заказ и т.д.
alchemyEngine.execute("CREATE TABLE hdbk.d_stock (id_in_stock SERIAL PRIMARY KEY, name_in_stock varchar)")

# справочник типа валюты
alchemyEngine.execute("CREATE TABLE hdbk.d_pricecurrency (id_pricecurrency SERIAL PRIMARY KEY, name_pricecurrency varchar)")
