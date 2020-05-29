package com.example.apps

import com.example.common.AppVariables._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object HeaderSelection {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val data = spark.sparkContext.textFile("data\\sample.txt").zipWithIndex().filter(x=>x._2>1).keys.map(x=>x.split(",")).map(x=>Row(x(0),x(1),x(2)))
    val schemas:Array[String] = spark.sparkContext.textFile("data\\sample.txt").zipWithIndex().filter(x=>x._2<2).keys.filter(x=>x.startsWith("A")).map(x=>x.split(",")).first()
    val validschema = StructType(schemas.map(fieldName=>StructField(fieldName, StringType, nullable = true)))

    //max value in a row using rdd api
    val maxinrow = spark.sparkContext.textFile("data\\sample.txt").zipWithIndex().filter(x=>x._2>1).keys.map(x=>x.split(",").max)

    //creating a df from rdd
    val df = spark.createDataFrame(data,validschema)

    //max value in a row after converting into df
    df.rdd.map(x=>x.mkString(",")).map(x=>x.split(",").max).foreach(println)

df.show()




  }}

