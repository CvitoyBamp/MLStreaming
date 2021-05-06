
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, VectorAssembler}

class ML (private val spark: SparkSession, private val dataPath: String){
  def training(modelPath: String) = {
    val data = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("spark.files.overwrite", "true")
      .csv(dataPath)

    val speciesIndexer = new StringIndexer()
      .setInputCol("species")
      .setOutputCol("indexedSpecies")
      .fit(data)

    val assembler = new VectorAssembler()
      .setHandleInvalid("skip")
      .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
      .setOutputCol("features")

    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))

    val dt = new RandomForestClassifier()
      .setLabelCol("indexedSpecies")
      .setFeaturesCol("features")

    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(speciesIndexer.labelsArray(0))

    val pipeline = new Pipeline()
      .setStages(Array(speciesIndexer, assembler, dt, labelConverter))

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)

    predictions.select("predictedLabel", "species", "features").show(5)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedSpecies")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println(s"Learned classification forest model:\n ${rfModel.toDebugString}")

    model.write.overwrite().save("D:\\scala\\MLStreaming\\pipeline\\")
  }
}