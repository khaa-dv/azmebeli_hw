import os
os.system('pip install beautifulsoup4')
os.system('pip install pandas')
os.system('pip install requests')
os.system('pip install psycopg2')
os.system('pip install sqlalchemy')


import time

try:
    message = 'Установка Postgres...'
    print(message)
    os.system('python ./pre_install.py')
    print('OK')
except:
    print('FAIL')

time.sleep(5)

try:
    message = 'Формирование структуры в базе данных...'
    print(message)
    os.system('python ./db_init.py')
    print('OK')
except:
    print('FAIL')

time.sleep(2)

try:
    message = 'Загрузка данных в базу...'
    print(message)
    os.system('python ./parse_site.py')
    print('OK. Данные загружены, можно загружать отчет в PowerBi')
except:
    print('FAIL')
