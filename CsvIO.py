import csv


def open_data_reader(file_name):
    try:
        data = open(file_name, "r", encoding="utf-8")
        reader = csv.DictReader(data)
        return reader
    except:
        from time import sleep
        from sys import exit
        print("Ошибка в имени или структуре файла {}".format(file_name))
        sleep(5)
        exit()


def open_data_writer(file_name, fields):
    try:
        file = open(file_name, "w", encoding="utf-8")
        if not type(fields) == type([0,0]):
            fields = [fields]
        writer = csv.DictWriter(file, fieldnames=fields)
        return writer
    except:
        from time import sleep
        from sys import exit
        print("Ошибка в имени или структуре файла {}".format(file_name))
        sleep(5)
        exit()


def clean_data(reader, processed_output_file, ip_column):
    # читаем данные и проверяем их

    temp = set()
    counter = 0
    for row in reader:
        temp.add(row[ip_column])
        counter += 1
    print("В обработке {0} IP".format(counter))

    # сохраняем список очищенных IP

    writer = open_data_writer(processed_output_file, ip_column)
    writer.writeheader()
    for row in temp:
        writer.writerow({ip_column: row})
    print("Уникальных: {0}".format(len(temp)))
    del temp

    # возвращаем доступ к очищенной таблице

    return open_data_reader(processed_output_file)


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