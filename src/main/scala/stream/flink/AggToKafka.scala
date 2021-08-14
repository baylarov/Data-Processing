package com.baylarov
package stream.flink

object AggToKafka extends App {
//  val env = StreamExecutionEnvironment.getExecutionEnvironment
//  val flinkKafkaProducer = new FlinkKafkaProducer[String]("localhost:9092", "sales", new SimpleStringSchema())
//
//  val inputDir = "D:\\data_processing\\input"
//  val dirOrders = s"$inputDir\\orders.json"
//  val jsonFile = FileInputFormat[String]
//
//  val inputDf = env.readTextFile(filePath = dirOrders)
//  env.addSource(inputDf)
//  println(inputDf)
//  val structuredIris = inputDf.map(line => {
//    val customer_id = line.split(",")(0)
//    val location = line.split(",")(1)
//    val order_date = line.split(",")(2)
//    val order_id = line.split(",")(3)
//    val price = line.split(",")(4).toDouble
//    val product_id = line.split(",")(5)
//    val seller_id = line.split(",")(6)
//    val status = line.split(",")(7)
//
//    Orders(customer_id, location, order_date, order_id, price, product_id, seller_id, status)
//  })

//  env.execute("Location Sales")
  case class Orders(customer_id: String, location: String, order_date: String, order_id: String, price: Double, product_id: String, seller_id: String, status: String)
}
