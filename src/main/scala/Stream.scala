import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.{col, concat_ws, from_csv, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class Stream (private val spark: SparkSession, private val modelPath: String){
    private val IRIS_LABELS =
    Map(0.0 -> "setosa", 1.0 -> "versicolor", 2.0 -> "virginica")

    private val PREDICTION_VALUE_UDF =
    udf((col: Double) => IRIS_LABELS(col))

    val IRIS_SCHEMA = StructType(
      StructField("sepal_length", DoubleType, nullable = true) ::
        StructField("sepal_width", DoubleType, nullable = true) ::
        StructField("petal_length", DoubleType, nullable = true) ::
        StructField("petal_width", DoubleType, nullable = true) ::
        Nil
    )

    def streaming() = {
      // Загружаем модель
      val model = PipelineModel.load(modelPath)

      import spark.implicits._

      // Читаем входной поток
      val input = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("failOnDataLoss", "false")
        .option("subscribe", "input")
        .load()
        .select($"value".cast(StringType))
        .withColumn("struct", from_csv($"value", IRIS_SCHEMA, Map("sep" -> ",")))
        .withColumn("sepal_length", $"struct".getField("sepal_length"))
        .withColumn("sepal_width", $"struct".getField("sepal_width"))
        .withColumn("petal_length", $"struct".getField("petal_length"))
        .withColumn("petal_width", $"struct".getField("petal_width"))
        .drop("value", "struct")

      // Применяем модель к входным данным
      val prediction = model.transform(input)

      // Выводим результат
      val query = prediction
        .withColumn(
          "predictedLabel",
          PREDICTION_VALUE_UDF(col("prediction"))
        )
        .select(
          $"predictedLabel".as("key"),
          concat_ws(",", $"sepal_length", $"sepal_width", $"petal_length", $"petal_width", $"predictedLabel").as("value")
        )
        .writeStream
        .outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("checkpointLocation", "D:\\scala\\MLStreaming\\checkpoint\\")
        .option("topic", "prediction")
        .start()

      query.awaitTermination()
    }
}
