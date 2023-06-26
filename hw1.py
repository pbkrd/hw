import pandas as pd
import sqlite3

FILE_JSON = "okved_2.json"
NEW_TABLE_NAME = "okved"


def main():
    connection = sqlite3.connect('hw1.db')
    cursor = connection.cursor()

    okved = pd.read_json(FILE_JSON)

    okved.to_sql(NEW_TABLE_NAME,
                 connection,
                 index=False,
                 if_exists='append')
    connection.commit()

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
