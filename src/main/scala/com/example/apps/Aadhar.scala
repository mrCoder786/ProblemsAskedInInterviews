package com.example.apps
import com.example.common.AppVariables._
import com.example.common.Schemas._
import spark.implicits._


object Aadhar {
  def main(args: Array[String]): Unit = {
    // val aadharData = spark.read.format("csv").option("inferSchema",true).option("header",true).load(auth)
    val aadharDataRDD = spark.sparkContext.textFile(path = auth)
      .map(x=>x.split(",")).map(x=>(x(2),x(3),x(128)))
      .filter(x=>x._2.matches("^[0-9]*$") && x._1>"650000" && x._3.!=("Delhi")).map(x=>x._2).take(10).foreach(println)
  }

}
