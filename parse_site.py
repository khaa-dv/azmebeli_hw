from config import URL_HOST, POSTGRES_PORT, USER_DB, PSWD_DB
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
import psycopg2
from sqlalchemy import create_engine

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'}
url = 'https://azbykamebeli.ru'
alchemyEngine = create_engine('postgresql+psycopg2://'+USER_DB+':'+PSWD_DB+'@'+URL_HOST+':'+POSTGRES_PORT+'/db_mebel', pool_recycle=3600);

# функция создания таблицы БД из таблицы pandas
def sql_table_update(name_table, df, exists_type='fail', schema_name='public', alchemyEngine=alchemyEngine):
    # exists_type определяет как отработает функция
    # exists_type='fail' создается новая таблица в БД, значение параметра по умолчанию
    # exists_type='append' добавление данных в таблицу БД, в конец таблицы
    # exists_type='replace' drop&create таблицы в БД

    dbConnection = alchemyEngine.connect();
    try:
        frame = df.to_sql(name_table, dbConnection, if_exists=exists_type, index=False, schema=schema_name)
    except ValueError as vx:
        result = vx
    except Exception as ex:
        result = ex
    else:
        result = "PostgreSQL Table %s has been successfully."%name_table
    finally:
        dbConnection.close();
    return result

# функция выгрузки таблицы pandas из БД
def sql_table_load(name_table, alchemyEngine=alchemyEngine):
    dbConnection = alchemyEngine.connect();
    query = "select * from " + name_table + ";"
    df = pd.read_sql(query, dbConnection);
    dbConnection.close()
    return df

# функция для произвольного запроса SQL
def sql_query(query, alchemyEngine=alchemyEngine):
    dbConnection = alchemyEngine.connect();
    df = pd.read_sql(query, dbConnection);
    dbConnection.close()
    return df

# функция обновления справочников
def f_dimention(df_trim, list_cols, table_name, schema_name="hdbk"):
    # выгружаем текущие значения справочника
    d_query = sql_query("SELECT "+", ".join(list_cols)+" FROM "+schema_name+"."+table_name)

    # определяем новые значения
    new_dim = df_trim[list_cols].merge(d_query[list_cols], how='outer', indicator=True)
    new_dim = new_dim[list_cols][new_dim['_merge'] == 'left_only'].drop_duplicates()

    # при наличии новых значений добавляем в справочник
    sql_table_update(table_name, new_dim, exists_type='append', schema_name=schema_name)

    # выгружаем справочник для преобразования данных
    d_query = sql_query("SELECT * FROM "+schema_name+"."+table_name)
    return d_query

def f_crt_divan_info(x):
    divan_info = x.find('div', {'class': 'btn btn-info btn-block buyoneclick mt-1'})

    # название
    name_divan = divan_info.get('data-name')

    # артикул
    num_artikul = int(divan_info.get('data-cid'))

    # цена без скидки
    find_price = x.find_all('a', {'class': 'store-price fake-link'})
    if len(find_price) != 0:
        price = float(''.join(re.findall("\d+", find_price[0].text)))
    else:
        price = float(divan_info.get('data-price'))

    # значение скидки
    price_discount = float(divan_info.get('data-price'))

    # тип валюты
    name_pricecurrency = x.find('meta', {'itemprop': 'priceCurrency'}).get('content')

    # в наличии или под заказ
    name_in_stock = x.find('small', {'class': 'badge'}).text

    # id дивана
    id_divan_from_source = int(divan_info.get('data-id'))

    # ссылка на источник
    url_divan = x.find('h4').find('a').get('href')

    t_one_divan = pd.DataFrame({
        'name_divan': [name_divan],
        'num_artikul': [num_artikul],
        'price': [price],
        'price_discount': [price_discount],
        'name_pricecurrency': [name_pricecurrency],
        'name_in_stock': [name_in_stock],
        'id_divan_from_source': [id_divan_from_source],
        'url_divan': [url_divan]
    })

    return t_one_divan

# находим ссылку для прямых диванов
r = requests.get(url, headers = headers)
soup = BeautifulSoup(r.text, features="lxml")
tree_links = soup.find('ul', {'id': 'mobile-menu'}).find_all('a')
for i in tree_links:
    if "Диваны прямые" in i:
        sub_url = i.get('href')

# находим ссылку для навигации по списку и число страниц с товаром
r = requests.get(url + sub_url, headers = headers)
soup = BeautifulSoup(r.text, features="lxml")
div_page = soup.find('nav', {'class': 'page-navigation'})
page_num = []
for i in div_page.find_all('a',  {'class': 'page-link'}):
    if i.get('href'):
        page_num.append(int(re.findall("\d+$", i.get('href'))[0]))
        url_page_nav = re.findall("(.*)\d+$", i.get('href'))[0]
max_page_num = max(page_num)

# парсим данные с сайта
df = pd.DataFrame()
for j in range(1, max_page_num+1):
    time.sleep(1)
    r = requests.get(url + url_page_nav + str(j), headers = headers)
    soup = BeautifulSoup(r.text, features="lxml")

    div_all_divan_page = soup.find('div', {'class': 'items-list'})
    div_list_divan = div_all_divan_page.find_all('div', {'class': 'col-lg-3 col-md-4 col-sm-6 mb-3 fadeInUp'})

    for i in div_list_divan:
        df = df.append(f_crt_divan_info(i), ignore_index=True)

# обновляем справочники
d_artikul = f_dimention(df, ['num_artikul', 'name_divan'], "d_artikul")
d_stock = f_dimention(df, ['name_in_stock'], "d_stock")
d_pricecurrency = f_dimention(df, ['name_pricecurrency'], "d_pricecurrency")

# загружаем данные в таблицу
df_denorm = df.merge(d_artikul).merge(d_stock).merge(d_pricecurrency)[['id_divan_from_source', 'id_artikul', 'id_in_stock',
       'price', 'price_discount', 'id_pricecurrency']]
df_denorm['date_snap'] = pd.to_datetime("today")
sql_table_update("t_divan", df_denorm, exists_type='append', schema_name='product')
