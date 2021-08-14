package com.baylarov
package batch.spark

import builder.SparkSessionBuilder

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._

class ThirdCase {
  def solution() {
    val sparkSessionBuilder = new SparkSessionBuilder
    val spark = sparkSessionBuilder.sparkBuilder("First Case")
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val inputDir = "D:\\data_processing\\input"
    val outputDir = "D:\\data_processing\\output"
    val dirOrders = s"$inputDir\\orders.json"
    val dirProducts = s"$inputDir\\products.json"
    val resultOutputPath = s"${outputDir}\\ThirdCase"

    val schemaOrders = Encoders.product[Orders].schema
    val schemaProducts = Encoders.product[Products].schema

    val dfOrders = spark.read.schema(schemaOrders).json(dirOrders)
      .withColumn("order_date_daily", col = to_timestamp(col("order_date")))
      .drop("order_date").withColumnRenamed("order_date_daily", "order_date")
      .select("product_id", "price", "order_date").distinct()

    dfOrders.createOrReplaceTempView("orders")

    lazy val dfResult = spark.sql(
      """
        |with tmp as (
        |select product_id, order_date, price,
        |lag(price) over (partition by product_id order by order_date) as previous_price
        |from orders )
        |
        |select product_id, order_date, price,
        |case when price = previous_price then 'same'
        |     when price > previous_price then 'rise'
        |     when price < previous_price then 'fall'
        |     else 'no info' end as price_changes
        |from tmp
        |order by order_date desc
        |""".stripMargin)


    println("Saving result as json...")
    dfResult.coalesce(1).write.mode("overwrite")
      .json(s"$resultOutputPath")
    println(s"Result saved as json to directory $resultOutputPath")
  }

  case class Orders(customer_id: String, location: String, order_date: String, order_id: String, price: Double, product_id: String, seller_id: String, status: String)

  case class Products(brandname: String, categoryname: String, productid: String, productname: String)
}
