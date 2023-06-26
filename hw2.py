import json
import zipfile
import sqlite3

from os import path
from datetime import datetime as dt

# ZIP_FILE = "egrul.json.zip"
CODE_TELECOM = "61"
ZIP_FILE = path.join("X:", "egrul.json.zip")
NEW_TABLE_NAME = 'telecom_companies'

def main():
    # Подключение к БД и создание курсора
    connection = sqlite3.connect("hw1.db")
    cursor = connection.cursor()

    # Удаление таблицы, если существует
    cursor.execute(f"DROP TABLE IF EXISTS {NEW_TABLE_NAME}")
    connection.commit()

    # Создание таблицы
    create_telecom_companies_table = f"""
                                         CREATE TABLE {NEW_TABLE_NAME}(
                                             inn INTEGER,
                                             title TEXT,
                                             code_okved VARCHAR(15),
                                             okved TEXT,
                                             release_date DATE
                                         )
                                         """
    cursor.execute(create_telecom_companies_table)
    connection.commit()

    with zipfile.ZipFile(ZIP_FILE, "r") as zip_obj:
        filenames = zip_obj.namelist()
        for filename in filenames:
            # Парсинг json
            with zip_obj.open(filename) as file:
                companies = json.load(file)
                for company in companies:
                    try:
                        props_okved = company["data"]["СвОКВЭД"]["СвОКВЭДОсн"]
                        code_okved = props_okved["КодОКВЭД"]
                        if code_okved.startswith(CODE_TELECOM):
                            inn = company["inn"]
                            title = company["name"]
                            okved = props_okved["НаимОКВЭД"]
                            release_date = dt.strptime(company["data"]["ДатаВып"], '%Y-%m-%d').date()
                        else:
                            raise ValueError
                    except (KeyError, ValueError):
                        pass
                    else:
                        # Выгрузка данных в БД
                        insert_data = f"""
                                          INSERT INTO {NEW_TABLE_NAME} (inn, title, code_okved, okved, release_date)
                                          VALUES (?, ?, ?, ?, ?)
                                          """
                        values = (inn, title, code_okved, okved, release_date)

                        cursor.execute(insert_data, values)
                        connection.commit()

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
