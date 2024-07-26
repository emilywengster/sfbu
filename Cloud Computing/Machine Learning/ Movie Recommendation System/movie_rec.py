from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import time

# Initialize Spark session
spark = SparkSession.builder.appName("MovieLensRecommendation").getOrCreate()

# Load the data (modify the path to your data location)
data_path = "gs://py1/ratings.csv"
ratings = spark.read.csv(data_path, header=True, inferSchema=True)

# Split the data into training and test sets
(train, test) = ratings.randomSplit([0.8, 0.2])

# ALS model
als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop", nonnegative=True)

# ParamGrid for Cross Validation
param_grid = ParamGridBuilder() \
    .addGrid(als.rank, [10, 20]) \
    .addGrid(als.maxIter, [5, 10]) \
    .addGrid(als.regParam, [0.01, 0.1]) \
    .build()

# Evaluator
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")

# Cross Validation
cv = CrossValidator(estimator=als, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=3, parallelism=2)

# Use a smaller subset of the training data
train_subset = train.sample(False, 0.05, seed=42)

start_time = time.time()

try:
    model = cv.fit(train_subset)
    elapsed_time = time.time() - start_time
    print(f"Model fitting completed in {elapsed_time:.2f} seconds")

    # Extract best model from the cv model above
    best_model = model.bestModel

except Exception as e:
    elapsed_time = time.time() - start_time
    print(f"Model fitting failed after {elapsed_time:.2f} seconds")
    print(f"Error: {e}")

# Evaluate the model
predictions = best_model.transform(test)
rmse = evaluator.evaluate(predictions)
print(f"Root-mean-square error = {rmse}")

# Save the best model
best_model.write().overwrite().save("gs://py1/output")
