package com.baylarov
package builder

import org.apache.spark.sql.SparkSession

class SparkSessionBuilder {
  def sparkBuilder(appName:String, master:String = "local[2]"): SparkSession = {
    val spark = SparkSession.builder().appName(appName).master(master).getOrCreate()
    spark
  }
}
