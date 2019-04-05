package com.sparkTutorial.machinelearning

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegression {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LogisticRegressionWithBFGSExample")
    val sc = new SparkContext(conf)

    //val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt").
  }
}

