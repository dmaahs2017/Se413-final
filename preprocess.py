from pyspark.sql import SparkSession
from pyspark import SparkContext


def main():
    spark = SparkSession.builder.config('spark.executor.memory', '4g').config('spark.driver.memory', '4g').getOrCreate()
    print("------------------------------")
    print(    spark.sparkContext.getConf().getAll())
    print("------------------------------")

    ham = spark.read.csv("ham.csv")
    spam = spark.read.csv("spam.csv")

    ham.createOrReplaceTempView("ham")
    spam.createOrReplaceTempView("spam")

    filter_query = """
            select _c0, int(_c1)
            from {table}
            where _c0 RLIKE '^[A-Z]+'
            order by _c1 desc
    """
    filtered_ham = spark.sql(filter_query.format(table='ham'))
    filtered_spam = spark.sql(filter_query.format(table='spam'))

    filtered_spam.createOrReplaceTempView('filtered_spam')
    filtered_ham.createOrReplaceTempView('filtered_ham')

    print("Example of dirty data in our raw ham set")
    spark.sql("""
            select _c0, int(_c1)
            from ham
            where _c0 not RLIKE "^[A-Z]+"
            order by _c1 desc
    """).show(10)

    print("Example of dirty data in our raw spam set")
    spark.sql("""
            select _c0, int(_c1)
            from spam
            where _c0 not RLIKE "^[A-Z]+"
            order by _c1 desc
    """).show(10)



    print("After cleaning the data, only words with alphabetical characters are in the set.")
    spark.sql("""
            select _c0, int(_c1)
            from filtered_ham
            where _c0 not RLIKE "^[A-Z]+"
            order by _c1 desc
    """).show(10)



    filtered_ham.coalesce(1).write.csv("ham_preprocessed.csv",mode='overwrite')
    filtered_spam.coalesce(1).write.csv("spam_preprocessed.csv",mode='overwrite')


if __name__ == '__main__':
    main()
