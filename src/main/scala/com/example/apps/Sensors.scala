package com.example.apps
import com.example.common.AppVariables._
import org.apache.spark.sql.expressions.Window
import com.example.common.Schemas._
import org.apache.spark.sql.functions._
import spark.implicits._

object Sensors {
  def main(args: Array[String]): Unit = {
    val sensorDf = spark.read.format("csv").option("header",true).schema(schema2).load(sensorpath)
    val windowS = Window.partitionBy("Mnemonic").orderBy("Mnemonic")
    sensorDf.withColumn("laggedData",lead("data",1).over(windowS))
      .withColumn("end_date",lead("timestamp",1).over(windowS))
      .withColumn("ind",when(col("data")===col("laggedData"),"false").otherwise("true"))
      .where(col("ind")==="true")
      .select(col("Sensor"),col("Mnemonic"),col("data"),col("timestamp").as("start_date"),col("end_date")).show(10)

  }

}
