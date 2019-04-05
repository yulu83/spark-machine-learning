package com.sparkTutorial.machinelearning

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object LinearRegressionModel {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
        .master("local[*]")
        .appName("LinearRegression")
        .getOrCreate()

    import spark.implicits._

    val training = spark.read.format("csv")
      .option("header", true)
      .option("mode", "DROPMALFORMED")
      .load("data/MQG.AX.csv")

    training.printSchema()

    val names = Seq("date", "close", "volume")
    val df = training.select("Date", "Close", "Volume")
        .map(row => (LocalDate.parse(row(0).toString, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay, row(1).toString.trim.toDouble, row(2).toString.toInt))
      .toDF(names: _*)

    val assembler1 = new VectorAssembler()
        .setInputCols(Array("date", "volume"))
        .setOutputCol("features")

    val assembled = assembler1.transform(df)

    assembled.show(10)

    val lir = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("close")
      .setRegParam(0.3)
      .setElasticNetParam(0)
      .setMaxIter(10)
      .setTol(1E-6)

    // Train the model
    val startTime = System.nanoTime()
    val lirModel = lir.fit(assembled)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    // Print the weights and intercept for linear regression.
    println(s"Weights: ${lirModel.coefficients} Intercept: ${lirModel.intercept}")

    val trainingSummary = lirModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")



    val testData = Seq((17560, 0, 816280)).toDF("date", "close", "volume")
    val testDF = assembler1.transform(testData)

    testDF.show()

    val predictionList = lirModel.transform(testDF)
        .select("prediction")
        .map(row => row(0).toString)
        .collectAsList()

    println(predictionList)

    lirModel.write.overwrite().save("model/linearRegressionModelMQ")

    spark.stop
  }

}
