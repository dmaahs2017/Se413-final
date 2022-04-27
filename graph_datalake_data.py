import csv
import mysql.connector
import argparse
from matplotlib import pyplot as plt


def query(sql, cursor):
    result = []
    cursor.execute(sql)
    row = cursor.fetchone()
    while row is not None:
        result.append(row)
        row = cursor.fetchone()
    return result


def query_result_to_parrellel_list(query_result):
    words = []
    freqs = []
    for word, freq in query_result:
        words.append(word)
        freqs.append(freq)
    return words, freqs

def main():
    mydb = mysql.connector.connect(
        host="35.226.180.173",
        user="root",
        password="Password1!",
        database="final",
    )
    cursor = mydb.cursor()

    top_spam_sql = f"SELECT * from spam where frequency < 6000 order by frequency desc limit 10"
    top_ham_sql = f"select * from ham where frequency < 6000 order by frequency desc limit 10"
    top_spam = query(top_spam_sql, cursor)
    top_ham = query(top_ham_sql, cursor)

    words, freqs = query_result_to_parrellel_list(top_spam)
    plt.figure(1)
    plt.title("Spam < 6000")
    plt.bar(words, freqs)

    words, freqs = query_result_to_parrellel_list(top_ham)
    plt.figure(2)
    plt.title("Ham < 6000")
    plt.bar(words, freqs)

    plt.show()
    


if __name__ == '__main__':
    main()
