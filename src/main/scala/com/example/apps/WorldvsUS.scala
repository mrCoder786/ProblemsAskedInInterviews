package com.example.apps
import com.example.common.AppVariables._
import com.example.common.Schemas._
import org.apache.spark.sql.functions._

object WorldvsUS {
  def main(args: Array[String]): Unit = {

    val barley = barleyWorld.join(barleyUS,Seq("year"),"left").withColumn("usa_barley_contribution%",pctProduction(col("ProductionUS"),col("ProductionROW"))).select(col("year"),col("ProductionROW").as("world_barley_harvest"),col("usa_barley_contribution%"))
    val beef = beefWorld.join(beefUS,Seq("year"),"left").withColumn("usa_beef_contribution%",pctProduction(col("ProductionUS"),col("ProductionROW"))).select(col("year"),col("ProductionROW").as("world_beef_slaughter"),col("usa_beef_contribution%"))
    val corn = cornWorld.join(cornUS,Seq("year"),"left").withColumn("usa_corn_contribution%",pctProduction(col("ProductionUS"),col("ProductionROW"))).select(col("year"),col("ProductionROW").as("world_corn_harvest"),col("usa_corn_contribution%"))
    val cotton = cottonWorld.join(cottonUS,Seq("year"),"left").withColumn("usa_cotton_contribution%",pctProduction(col("ProductionUS"),col("ProductionROW"))).select(col("year"),col("ProductionROW").as("world_cotton_harvest"),col("usa_cotton_contribution%"))


    barley.join(beef,Seq("year"),"left").join(corn,Seq("year"),"left").join(cotton,Seq("year"),"left").show()
  }

  val barleyUS = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "'Barley'!A494:D506").option("useHeader",false).option("inferSchema", false).schema(schemaUS).option("treatEmptyValuesAsNulls", "false").load(baseline)
  val barleyWorld = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "'Barley'!A512:D524").option("useHeader",false).option("inferSchema", false).schema(schemaROW).option("treatEmptyValuesAsNulls", "false").load(baseline)
  val beefUS = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "'Beef'!A584:D596").option("useHeader",false).option("inferSchema", false).schema(schemaUS).option("treatEmptyValuesAsNulls", "false").load(baseline)
  val beefWorld = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "'Beef'!A602:D614").option("useHeader",false).option("inferSchema", false).schema(schemaROW).option("treatEmptyValuesAsNulls", "false").load(baseline)
  val cornUS = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "'Corn'!A638:D650").option("useHeader",false).option("inferSchema", false).schema(schemaUS).option("treatEmptyValuesAsNulls", "false").load(baseline)
  val cornWorld = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "'Corn'!A674:D686").option("useHeader",false).option("inferSchema", false).schema(schemaROW).option("treatEmptyValuesAsNulls", "false").load(baseline)
  val cottonUS = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "'Cotton'!A530:D542").option("useHeader",false).option("inferSchema", false).schema(schemaUS).option("treatEmptyValuesAsNulls", "false").load(baseline)
  val cottonWorld = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "'Cotton'!A566:D578").option("useHeader",false).option("inferSchema", false).schema(schemaROW).option("treatEmptyValuesAsNulls", "false").load(baseline)

}
