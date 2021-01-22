from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, IndexToString
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType


def build_spark_session():
    return SparkSession.builder.appName("Logistic Regression").getOrCreate()


spark = build_spark_session()


# Schema for iris dataset
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
    iris_df = spark.read.schema(get_iris_schema()).csv(input_file)
    return iris_df


# to convert the string classes to double type
def string_to_index(data, input_col="class", output_col="indexed_class"):
    # iris_data = read_iris_data_frame()
    indexer = StringIndexer(inputCol=input_col, outputCol=output_col).fit(data)
    indexed_data = indexer.transform(data)
    return indexed_data


# converts to feature vetor
def convert_to_vector():
    vectorizer = VectorAssembler(
        inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"],
        outputCol="features"
    )
    return vectorizer


# converts the converted class indexes to index
def index_to_string(data, input_col="prediction", output_col="class"):
    # indexed_data could be broad casted, had some issues in my local spark, so reading the data frame again
    indexed_data = string_to_index(read_iris_data_frame(), "class", "labels").select(["class", "labels"]).distinct()
    result = data.join(indexed_data, data.prediction == indexed_data.labels)
    result.select("class").show()
    return result.select("class")


# for cleansing the data to the standard format
def data_cleanse(data):
    iris_data = string_to_index(data, "class", "indexed_class")
    # iris_data.show(5)
    iris_df_assembler = convert_to_vector()
    iris_data = iris_df_assembler.transform(iris_data)
    iris_data = iris_data.select(["features", "indexed_class"])
    return iris_data


# model and model parameters
def build_model(data):
    model_df = data
    train_data, test_data = model_df.randomSplit([0.80, 0.20])
    log_reg_model = LogisticRegression(labelCol="indexed_class") \
        .setParams(tol=0.0001, regParam=0.0, maxIter=100, fitIntercept=True) \
        .fit(train_data)
    return train_data, test_data, log_reg_model


# test data frame
def create_test_data():
    pred_data = build_spark_session().createDataFrame(
        [(5.1, 3.5, 1.4, 0.2),
         (6.2, 3.4, 5.4, 2.3)],
        ["sepal_length", "sepal_width", "petal_length", "petal_width"])
    return pred_data


# evaluate the results
def evaluation_metrics(predicted_results):
    evaluator = MulticlassClassificationEvaluator() \
        .setLabelCol("indexed_class") \
        .setPredictionCol("prediction") \
        .setMetricName("accuracy")

    accuracy = evaluator.evaluate(predicted_results)
    print("Accuracy: " + str(accuracy))


# predict the test results
def predict_test_results(lr_model, pred_data):
    pred_data = pred_data
    pred_df_assembler = convert_to_vector()
    pred_data = pred_df_assembler.transform(pred_data)
    pred_data = pred_data.select("features")
    model = lr_model.transform(pred_data)
    result = index_to_string(model)
    return result


def write_to_csv(result):
    result.repartition(1).write. \
        format("csv"). \
        option("header", "true"). \
        mode(saveMode="Overwrite"). \
        save("out/out_3_2.txt")


def main():
    iris_data = read_iris_data_frame("iris.data")
    iris_data = data_cleanse(iris_data)
    train_data, test_data, log_reg_model = build_model(iris_data)
    pred_data = create_test_data()
    predicted_results = log_reg_model.transform(test_data)
    evaluation_metrics(predicted_results)
    prediction = predict_test_results(log_reg_model, pred_data)
    write_to_csv(prediction)


if __name__ == '__main__':
    main()
