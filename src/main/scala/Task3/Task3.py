from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType


def build_spark_session():
    spark = SparkSession.builder.appName("Logistic Regression").getOrCreate()
    return spark


def get_iris_schema():
    iris_schema = StructType([
        StructField("sepal_length", DoubleType()),
        StructField("sepal_width", DoubleType()),
        StructField("petal_length", DoubleType()),
        StructField("petal_width", DoubleType()),
        StructField("class", StringType())
    ])
    return iris_schema


def read_iris_data_frame(input_file="iris.data"):
    iris_df = build_spark_session().read.schema(get_iris_schema()).csv(input_file)
    return iris_df


def data_cleanse():
    iris_data = read_iris_data_frame()
    class_indexer = StringIndexer(inputCol="class", outputCol="indexed_class").fit(iris_data)
    iris_data = class_indexer.transform(iris_data)
    # iris_data.show(5)
    iris_df_assembler = VectorAssembler(
        inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        outputCol="features"
    )
    iris_data = iris_df_assembler.transform(iris_data)
    # iris_data.printSchema()
    iris_data = iris_data.select(["features", "indexed_class"])
    return iris_data


def build_model():
    model_df = data_cleanse()
    train_data, test_data = model_df.randomSplit([0.80, 0.20])
    log_reg_model = LogisticRegression(labelCol="indexed_class")\
        .setParams(tol=0.0001, regParam=0.0, maxIter=100, fitIntercept=True)\
        .fit(train_data)
    return train_data, test_data, log_reg_model


def evaluation_metrics(predicted_results):
    evaluator = MulticlassClassificationEvaluator() \
        .setLabelCol("indexed_class") \
        .setPredictionCol("prediction") \
        .setMetricName("accuracy")

    accuracy = evaluator.evaluate(predicted_results)
    print("Accuracy: " + str(accuracy))


def main():
    train_data, test_data, log_reg_model = build_model()
    predicted_results = log_reg_model.transform(test_data)
    evaluation_metrics(predicted_results)


if __name__ == '__main__':
    main()
