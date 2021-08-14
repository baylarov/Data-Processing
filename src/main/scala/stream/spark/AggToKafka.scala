package com.baylarov
package stream.spark

import builder.SparkSessionBuilder

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object AggToKafka extends App {
  val sparkSessionBuilder = new SparkSessionBuilder
  val spark = sparkSessionBuilder.sparkBuilder("Fourth Case")
  spark.sparkContext.setLogLevel("ERROR")

  val dirOrders = "D:\\data_processing\\input\\streaming\\orders"
  val schemaOrders = Encoders.product[Orders].schema

  val dfOrders = spark.readStream
    .schema(schemaOrders)
    .json(s"$dirOrders")
    .withColumn("order_date_time", to_timestamp(col("order_date")))
    .withWatermark("order_date_time", "20 minutes")
    .as("o")

  lazy val dfWindowing = dfOrders
    .withWatermark("order_date_time", "20 minutes")
    .groupBy(window(col("order_date_time"), "10 minutes"),
      col("location"))

  lazy val dfAgg = dfWindowing.agg(
    count("product_id").as("product_count"),
    approx_count_distinct("seller_id").as("distinct_seller_count"))
    .select("location", "product_count", "distinct_seller_count", "window")

  lazy val dfValue = dfAgg
    .withColumn("value",
      to_json(struct("location", "product_count", "distinct_seller_count", "window")))
    .select("value")

  dfValue
    .writeStream
    .format("kafka")
    .trigger(Trigger.ProcessingTime("20 seconds"))
    .outputMode("update")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "sales")
    .option("checkpointLocation", "D:\\Programs\\kafka\\checkpoints\\agg")
    .start()
    .awaitTermination()

  case class Orders(customer_id: String, location: String, order_date: String, order_id: String, price: Double, product_id: String, seller_id: String, status: String)

}
