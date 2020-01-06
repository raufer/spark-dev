from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").getOrCreate()

data = [
    ('Thin', 'Cell phone', 6000),
    ('Normal', 'Tablet', 1500),
    ('Mini', 'Tablet', 5500),
    ('Ultra thin', 'Cell phone', 5000),
    ('Vey thin', 'Cell phone', 6000),
    ('Big', 'Tablet', 2500),
    ('Bendable', 'Cell phone', 3000),
    ('Foldable', 'Cell phone', 3000),
    ('Pro', 'Tablet', 5400),
    ('Pro2', 'Tablet', 6500)
]

products = spark.createDataFrame(data, ['product', 'category', 'revenue'])

products.show()