package com.baylarov
package batch.spark

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{count, desc}

class ToElasticsearch {
  def solution(): Unit = {
    val spark = SparkSession.builder()
      .appName("ES2Spark").master("local[2]")
      .config("spark.es.nodes", "localhost")
      .config("spark.es.port", "9200")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val dirInput = "D:\\data_processing\\input\\sample.csv"
    val schema = Encoders.product[Sessions].schema
    val esPath = "baylarov/sales_frequency"
    val droppedFields = Seq("referrer", "current_url", "page_type", "product_category", "image_url", "col_1", "col_2",
      "col_3", "col_4", "currency")

    lazy val dfInput = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .csv(dirInput)
      .drop(droppedFields: _*)

    val dfCleaned = dfInput.na.replace(dfInput.columns.toSeq, Map("\\N" -> null)).where("product_id is not null")

    try {
      assert(dfCleaned.count() > 0, "Could not proceed with empty datasets.")

      lazy val dfResult = dfCleaned.as("a")
        .join(dfCleaned.as("b"), Seq("session_id"), "inner")
        .groupBy("a.product_id", "b.product_id")
        .agg(count("a.session_id").as("frequency"))
        .where(
          """a.product_id < b.product_id
            |and length(b.product_id) = length(a.product_id)
            |and length(a.product_id) <= 7""".stripMargin)
        .orderBy(desc("frequency"))
        .selectExpr("concat(a.product_id, b.product_id) as id", "a.product_id as product_id_1", "b.product_id as product_id_2", "frequency")

      println("Saving results ...")

      dfResult.write
        .format("org.elasticsearch.spark.sql")
        .option("es.mapping.id", "id")
        .option("es.write.operation", "upsert")
        .mode("append")
        .save(esPath)

      println(s"${dfResult.count()} records saved in '$esPath'.")
    }
    catch {
      case exc: AssertionError => println(s"Error encountered:\n$exc")
    }
    finally {
      spark.stop()
    }

  }
  case class Sessions(date: String, referrer: String, current_url: String, page_type: String, product_category: String,
                      product_name: String, image_url: String, col_1: String, col_2: String, col_3: String, currency: String,
                      cart_amount: String, col_4: String, user_id: String, session_id: String, year: String, month: String,
                      day: String, hour: String, product_id: String)
}
