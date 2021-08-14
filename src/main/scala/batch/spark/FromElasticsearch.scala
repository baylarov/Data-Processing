package com.baylarov
package batch.spark

import org.apache.spark.sql.SparkSession

class FromElasticsearch {
  def solution(): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark2ES").master("local[2]")
      .config("spark.es.nodes", "localhost")
      .config("spark.es.port", "9200")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    lazy val df = spark.read
      .format("org.elasticsearch.spark.sql")
      .load("baylarov/sales_frequency")

    df.printSchema()
  }

}
