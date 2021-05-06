import org.apache.spark.sql.SparkSession

object Main extends App{

  val spark = SparkSession.builder()
    .appName("Ирисы Фишера")
    .master("local[*]")
    .getOrCreate()

  val dataPath = "D:\\scala\\MLStreaming\\data\\IRIS.csv"

  val modelPath = "D:\\scala\\MLStreaming\\pipeline\\"

  val irisTrainModel = new ML(spark, dataPath)

  irisTrainModel.training(modelPath)

  val irisStreaming = new Stream(spark, modelPath)

  irisStreaming.streaming()

}
