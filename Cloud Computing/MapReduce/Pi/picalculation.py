from pyspark.sql import SparkSession
import random

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PiCalculation").getOrCreate()
    sc = spark.sparkContext

    num_samples = 1000000
    count = sc.parallelize(range(0, num_samples)).filter(inside).count()
    pi = 4 * count / num_samples
    print(f"Pi is roughly {pi}")

    spark.stop()
