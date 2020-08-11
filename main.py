import configparser
import os
from random import choice
from json import loads
from CsvIO import open_data_reader, open_data_writer, clean_data


def close():  # Функция заверешния работы
    from time import sleep
    sleep(5)
    from sys import exit
    exit(1)


try:
    import requests
except ImportError:
    try:
        os.system("pip3 install -r " + os.path.abspath(__file__) + "\\\\..\\\\requirements.txt")
        import requests
    except:
        try:
            os.system("pip install -r requirements.txt")
            import requests
        except:
            print("Проверьте наличие библиотеки requests и повторите попытку")
            close()


def add_data(ans, error_writer):
    global writer, ip_column, city_column
    for row in ans:
        if row["city"] is None:
            error_writer.writerow({ip_column: row["ip"]})
            continue
        if row["city"]["name_ru"] == "":
            temp = row["country"]["capital_ru"]
            row['city'] = {"name_ru": temp}
        writer.writerow({ip_column: row["ip"], city_column: row["city"]["name_ru"]})


def get(query, main_url, keys):
    key = choice(keys)
    try:
        # Делаем запрос по группе IP
        request = requests.get(main_url(key, query))
        if request.status_code // 100 == 2:  # Проверяем успешность запроса
           ans = loads(request.text)  # Парсим JSON
        else:
            return
    except requests.exceptions.RequestException:
        print("Проверьте ваше подключение к сети и повторите попытку")
        close()
    except:
        print("Ошибка подключения к удаленной базе данных")
        close()
    if not type(ans) == type([0,0]):
        ans = [ans]
    add_data(ans, error_writer)  # Добавляем результат в выходной файл


try:
    # Открываем конфигурационный файл
    config = configparser.ConfigParser()
    config.read("config.ini")

    # Читаем данные из файла настроек
    error_name = config["FILES"]["error_file"]
    file_name = config["FILES"]["input_file"]
    processed_name = config["FILES"]["interim_file"]
    final_name = config["FILES"]["output_file"]
    ip_column = config["COLUMNS"]["ip_column"]
    city_column = config["COLUMNS"]["city_column"]
    max_count = int(config["META"]["step"])

    main_url = "http://api.sypexgeo.net/{0}/json/{1}".format
    keys = ["D9J0f", "C09Jv", "odUYny", "ecHNG", "jiCkY"]
except configparser.NoSectionError:
    print("Не обнаружены секции конфигурационного файла")
    close()
except configparser.NoOptionError:
    print("Не обнаружены некоторые концигурационные опции. Устраните проблему, используя readme.txt")
    close()
except KeyError:
    print("Не обнаружены некоторые обязательные концигурационные опции. Устраните проблему, используя readme.txt")
except Exception:
    print("Проблема с конфигурационным файлом")
    close()

# Открываем выходной файл
writer = open_data_writer(final_name, [ip_column, city_column])
writer.writeheader()

# Открываем файл ошибок
error_writer = open_data_writer(error_name, [ip_column])
error_writer.writeheader()

# Читаем данные
data = open_data_reader(file_name)

# Удаляем дубликаты
data = clean_data(data, processed_name, ip_column)

# Распознаем IP адреса
query = ""
counter = 0
for row in data:
    query += row[ip_column] + ","
    counter += 1
    if counter > max_count:
        counter = 0
        query = query[:len(query)-1]
        get(query, main_url, keys)
        query = ''
if counter > 0:
    query = query[:len(query) - 1]
    get(query, main_url, keys)