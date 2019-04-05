# spark-machine-learing
The project was originally taken from https://github.com/jleetutorial/scala-spark-tutorial.git

Based on the existing code base, i have tried adding a Kafka Streaming example,

Build a simple linear regression model and save the model onto disk. (refer to com/sparkTutorial/machinelearning/LinearRegressionModel.scala

The Streaming piple line demostrates a flow like the following:

Kafka input topic -> Spark (apply linear regression model to do prediction) -> Kafka output topic

The Kafka streaming code can be found in com/sparkTutorial/streaming/StreamingKafkaExample.scala
