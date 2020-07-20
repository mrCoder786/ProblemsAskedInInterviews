package com.example.apps
import com.example.common.AppVariables._
import com.example.common.Schemas._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
/* Problem1: Question2 (using Spark) */

object PovertyProblem {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val povertyDf = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "'Poverty Data 2018'!A6:AH3198").option("useHeader",false).option("inferSchema", false).schema(schema1).option("treatEmptyValuesAsNulls", "false").load(povertyEstimates)
    val statesDf = spark.read.format("com.crealytics.spark.excel").option("useHeader",true).option("inferSchema", true).option("treatEmptyValuesAsNulls", true).option("sheetName", "Sheet2").load(states)

    val reportDf = povertyDf.join(statesDf,povertyDf("State")===statesDf("Postal Abbreviation"),"left")
      .withColumn("AreaWithAbb",concat_ws(" ",col("State"),col("Area_name")))
      .withColumn("POV_elder_than17_2018",over17(col("PCTPOVALL_2018"),col("PCTPOV017_2018")))
      .where((col("Urban_Influence_Code_2003")%2===1) && (col("Rural-urban_Continuum_Code_2013")%2===0))
      .select(col("Capital Name").as("State"),col("AreaWithAbb").as("Area_name"),col("Urban_Influence_Code_2003"),col("Rural-urban_Continuum_Code_2003"),col("POV_elder_than17_2018"))

    //showing 10 records of required result
    reportDf.show(10)


  }

}

