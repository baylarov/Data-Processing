package com.baylarov
package batch.spark

import builder.SparkSessionBuilder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.{coalesce, col, count, desc, sum, to_date, to_timestamp}

class SecondCase {
  def solution() {
    val sparkSessionBuilder = new SparkSessionBuilder
    val spark = sparkSessionBuilder.sparkBuilder("First Case")
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val inputDir = "D:\\data_processing\\input"
    val outputDir = "D:\\data_processing\\output"
    val dirOrders = s"$inputDir\\orders.json"
    val dirProducts = s"$inputDir\\products.json"
    val resultOutputPath = s"${outputDir}\\SecondCase"

    val schemaOrders = Encoders.product[Orders].schema
    val schemaProducts = Encoders.product[Products].schema

    val dfOrders = spark.read.schema(schemaOrders).json(dirOrders)
      .withColumn("order_date_daily", col = to_date(to_timestamp(col("order_date")), "yyyy-MM-dd"))
      .drop("order_date").withColumnRenamed("order_date_daily", "order_date")
    val dfProducts = spark.read.schema(schemaProducts).json(dirProducts).as("p")

    lazy val dfSuccessOrders = dfOrders
      .where("status in ('Created')")
      .groupBy("product_id", "order_date", "seller_id")
      .agg(sum("price").as("total_created_price")
        , count("order_id").as("total_created_amount")).as("s")

    lazy val dfUnSuccessOrders = dfOrders
      .where("status in ('Cancelled','Returned')")
      .groupBy("product_id", "order_date", "seller_id")
      .agg(sum("price").as("total_cancelled_price")
        , count("order_id").as("total_cancelled_amount")).as("u")

    val droppedColumns_1 = List[String]("order_date", "seller_id", "product_id")

    lazy val dfFull = dfSuccessOrders
      .join(dfUnSuccessOrders, col("s.product_id") === col("u.product_id")
        and col("s.seller_id") === col("u.seller_id")
        and col("s.order_date") === col("u.order_date"), "full")
      .na.fill(0, Array("s.total_created_price", "u.total_cancelled_price", "s.total_created_amount", "u.total_cancelled_amount"))
      .withColumn("productid", coalesce(col("s.product_id"), col("u.product_id")))
      .withColumn("orderdate", coalesce(col("s.order_date"), col("u.order_date")))
      .withColumn("sellerid", coalesce(col("s.seller_id"), col("u.seller_id")))
      .drop(colNames = droppedColumns_1: _*)
      .withColumnRenamed("productid", "product_id")
      .withColumnRenamed("sellerid", "seller_id")
      .withColumnRenamed("orderdate", "order_date")
      .orderBy(desc("total_created_price"))
      .as("f")

    val droppedColumns_2 = List[String]("total_created_price", "total_created_amount", "total_cancelled_price", "total_cancelled_amount")
    lazy val dailySalesInfo = dfFull
      .withColumn("net_sales_amount", col("total_created_amount") - col("total_cancelled_amount"))
      .withColumn("net_sales_price", col("total_created_price") - col("total_cancelled_price"))
      .withColumn("gross_sales_amount", col("total_created_amount"))
      .withColumn("gross_sales_price", col("total_created_price"))
      .join(dfProducts, col("f.product_id") === col("p.productid"), "left")
      .withColumnRenamed("categoryname", "category_name")
      .select("order_date", "seller_id", "product_id", "category_name", "net_sales_amount", "net_sales_price", "gross_sales_amount", "gross_sales_price")

    dailySalesInfo.createOrReplaceTempView("sales")

    lazy val topSellers = spark.sql(
      """
        |with tmp as (
        |select order_date, seller_id, sum(net_sales_price) as net_sales_price,
        |sum(gross_sales_amount) as gross_sales_amount,
        |row_number() over (partition by order_date order by sum(net_sales_price) desc) as ranking
        |from sales
        |group by order_date, seller_id
        |)
        |
        |select order_date, seller_id, gross_sales_amount  from tmp
        |where ranking<=10
        |""".stripMargin).as("ts")

    lazy val topSellersProducts = spark.sql(
      """
        |with tmp as (
        |select order_date, seller_id, category_name,
        |row_number() over (partition by order_date, seller_id order by sum(net_sales_price) desc) as ranking
        |from sales
        |group by order_date, seller_id, category_name
        |)
        |
        |select order_date, seller_id, category_name from tmp
        |where ranking = 1
        |""".stripMargin).as("tp")

    lazy val dfResult = topSellers
      .join(topSellersProducts, Seq("order_date", "seller_id"))
      .drop("ranking")

    println("Saving results as json...")
    dfResult.coalesce(1).write
      .mode("overwrite")
      .json(s"$resultOutputPath")
    println(s"Reults saved as json to directory ${resultOutputPath}")
  }

  case class Orders(customer_id: String, location: String, order_date: String, order_id: String, price: Double, product_id: String, seller_id: String, status: String)

  case class Products(brandname: String, categoryname: String, productid: String, productname: String)
}

