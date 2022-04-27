import csv
import mysql.connector
import argparse


def main():
    parser = argparse.ArgumentParser();
    parser.add_argument("input_file")
    parser.add_argument("table")
    args = parser.parse_args()

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="iw1MbhCngTP42o",
        database="emails",
    )
    cursor = mydb.cursor()

    with open(args.input_file) as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            word = row[0]
            count = row[1]

            sql = f"INSERT INTO {args.table} (word, frequency) VALUES (%s, %s)"
            values = (word, count)
            cursor.execute(sql, values)
            mydb.commit()
            print(cursor.rowcount, f"record inserted. ({word}, {count})")


if __name__ == '__main__':
    main()
