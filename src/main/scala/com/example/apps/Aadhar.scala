package com.example.apps
import com.example.common.AppVariables._
import org.apache.log4j.{Level, Logger}

/* Problem2: Question2 (using Spark) */

object
Aadhar {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //displaying 10 records of required result
    val aadharDataRDD = spark.sparkContext.textFile(path = auth)
      .map(x=>x.split(",")).map(x=>(x(2),x(3),x(128)))
      .filter(x=>x._2.matches("^[0-9]*$") && x._1>"650000" && x._3.!=("Delhi")).map(x=>x._2).take(10).foreach(println)
  }

}
