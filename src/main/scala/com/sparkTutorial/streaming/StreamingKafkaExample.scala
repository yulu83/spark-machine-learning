package com.sparkTutorial.streaming

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.UUID

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.functions._

case class Record(date: Long, close: Double, volume: Long)

object StreamingKafkaExample {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("KafkaExample").getOrCreate()
      //StreamingContext("local[*]", "KafkaExample", Seconds(5))
    val sc = spark.sparkContext;
    val ssc = new StreamingContext(sc, Seconds(5))

    val assembler1 = new VectorAssembler()
      .setInputCols(Array("date", "volume"))
      .setOutputCol("features")

    val lirModel = LinearRegressionModel.load("model/linearRegressionModelMQ")

    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id" -> "regression_input",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer])

    val topics = List("regression")

    import spark.implicits._
    // map the message part
    // first element of tuple is the topic string


    def row(line: List[String]): Row = Row(LocalDate.parse(line(0), DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay,
                                           line(1).toDouble,
                                           line(2).toInt)

    def dfSchema: StructType = {
      StructType(
        Seq(
          StructField("date", LongType, false),
          StructField("close", DoubleType, false),
          StructField("volume", IntegerType, false)
        )
      )
    }


//    val lines = KafkaUtils.createDirectStream[String, String](
//      ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)
//    )

    val outTopics = List("regression-out")


    val lines = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "regression")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .as[String]

    lines.printSchema()

    val dataFrame = lines
      .transform(ds => {
          val df = ds.map(d => {
            val attribute = d.split(",")
            if(attribute.isEmpty) {
              Record(0, 0, 0)
            } else {
              Record(LocalDate.parse(attribute(0), DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay, attribute(1).toDouble, attribute(2).toLong)
            }
          }).toDF()

          val testDF = assembler1.transform(df)
          val prediction = lirModel.transform(testDF)
          prediction.withColumnRenamed("prediction", "value")
          .selectExpr("cast(value as STRING)")
          //testDF
    })

    val consoleQuery = startConsoleStream(dataFrame)
    val outputQuery = startOutputStream(dataFrame)


//    lines.foreachRDD(rdd => {
//      if(!rdd.isEmpty()) {
//        val rddToRow = rdd.map(r => r.value.split(",").to[List]).map(row)
//        val df = spark.createDataFrame(rddToRow, dfSchema)
//        df.show()
//
//        val testDF = assembler1.transform(df)
//        testDF.show()
//
//        println("prediction=========")
//
//        val prediction = lirModel.transform(testDF)
//        prediction.show()
//      }
//    })


    // ctrl + c to terminate
    //ssc.start()
    ssc.awaitTermination()
  }

  private def startOutputStream(messages: DataFrame): StreamingQuery = {
    messages.writeStream
       // .outputMode("complete")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "regression-out")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()
  }

  private def startConsoleStream(messages: DataFrame): StreamingQuery = {
    messages.writeStream
      // .outputMode("complete")
      .format("console")
      .start()
  }
}


