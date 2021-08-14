package com.baylarov
package batch.spark

import builder.SparkSessionBuilder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._

class FirstCase {
  def solution():Unit = {
    val sparkSessionBuilder = new SparkSessionBuilder
    val spark = sparkSessionBuilder.sparkBuilder("First Case")
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val inputDir = "D:\\data_processing\\input"
    val outputDir = "D:\\data_processing\\output"
    val dirOrders = s"$inputDir\\orders.json"
    val dirProducts = s"$inputDir\\products.json"

    val schemaOrders = Encoders.product[Orders].schema
    val schemaProducts = Encoders.product[Products].schema

    val dfOrders = spark.read.schema(schemaOrders).json(dirOrders)
    val dfProducts = spark.read.schema(schemaProducts).json(dirProducts)

    lazy val dfSuccessOrders = dfOrders
      .where("status in ('Created')")
      .groupBy("product_id")
      .agg(sum("price").as("total_created_price")
        , count("order_id").as("total_created_amount")).as("s")

    lazy val dfUnSuccessOrders = dfOrders
      .where("status in ('Cancelled','Returned')")
      .groupBy("product_id")
      .agg(sum("price").as("total_cancelled_price")
        , count("order_id").as("total_cancelled_amount")).as("u")

    dfProducts.createOrReplaceTempView("products")
    dfOrders.createOrReplaceTempView("orders")

    lazy val dfFull = dfSuccessOrders
      .join(dfUnSuccessOrders, col("s.product_id") === col("u.product_id"), "full")
      .na.fill(0, Array("s.total_created_price", "u.total_cancelled_price", "s.total_created_amount", "u.total_cancelled_amount"))
      .withColumn("productid", coalesce(col("s.product_id"), col("u.product_id")))
      .drop(colNames = "product_id")

    val droppedColumns = List[String]("total_created_price", "total_created_amount", "total_cancelled_price", "total_cancelled_amount")
    lazy val salesPerProduct = dfFull
      .withColumn("net_sales_amount", col("total_created_amount") - col("total_cancelled_amount"))
      .withColumn("net_sales_price", col("total_created_price") - col("total_cancelled_price"))
      .withColumn("gross_sales_amount", col("total_created_amount"))
      .withColumn("gross_sales_price", col("total_created_price"))
      .drop(droppedColumns: _*).as("s")

    val days = 5
    lazy val avgTotalAmounts = spark.sql(
      s"""
         |with tmp as (
         |select product_id, order_id,
         |row_number() over (partition by product_id order by order_date desc) as ranking
         |from orders
         |where status = 'Created'
         |)
         |
         |select product_id, count(order_id)/$days as avg_sales_amount
         |from tmp
         |where ranking<=$days
         |group by product_id
         |""".stripMargin).as("f")

    lazy val mostSellingLocations = spark.sql(
      """
        |with loc as (
        |select product_id, location, count(order_id) as total_sales_amount,
        |row_number() over (partition by product_id order by count(order_id) desc) as ranking
        |from orders
        |where status = 'Created'
        |group by product_id, location
        |)
        |select product_id, location as top_selling_location
        |from loc where ranking = 1
        |""".stripMargin).as("l")


    lazy val resultDf = salesPerProduct
      .join(avgTotalAmounts, col("s.productid") === col("f.product_id"), "left")
      .join(mostSellingLocations, col("s.productid") === col("l.product_id"), "left")
      .na.fill(0, Array("avg_sales_amount"))
      .na.fill("No city", Array("top_selling_location"))
      .drop("product_id")
      .orderBy(desc("gross_sales_amount"))

    println("=" * 50)
    println("Saving results as json...")

    resultDf.coalesce(1).write
      .mode("overwrite")
      .json(s"$outputDir\\FirstCase")

    println("Result saved as json.")

  }

  case class Orders(customer_id: String, location: String, order_date: String, order_id: String, price: Double, product_id: String, seller_id: String, status: String)

  case class Products(brandname: String, categoryname: String, productid: String, productname: String)
}
