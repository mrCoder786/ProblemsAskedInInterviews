package com.example.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object AppVariables {
  val spark = SparkSession.builder.master("local").appName("Povert Problem").getOrCreate()
  val povertyEstimates = "data\\PovertyEstimates.xls"
  val states = "data\\StatesName.xlsx"
  val auth = "data\\auth.csv"
  val sensorpath = "data\\sensor.txt"
  val baseline = "data\\InternationalBaseline2019-Final.xlsx"
  def over17 = udf { (col1: Long, col2: Long) => {col1 - (col2 / 100 * col1)}}
  def pctProduction = udf { (col1: Float, col2: Float) => {col1 / col2 * 100}}

}
