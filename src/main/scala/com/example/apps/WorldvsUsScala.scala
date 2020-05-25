package com.example.apps
import com.example.common.AppVariables._
import  com.example.common.Schemas._
import java.io.{File, FileInputStream}
import scala.collection.JavaConversions._
import com.example.common.Schemas.BarleyUS
import org.apache.poi.xssf.usermodel.XSSFWorkbook

object WorldvsUsScala {
  def main(args:Array[String]): Unit ={

    val barleyUS = new BarleyUSExcelReader(baseline,493,505).streadExcel().map(x=>(x.year,x.ProductionUS)).toMap
    val barleyROW = new BarleyUSExcelReader(baseline,511,523).streadExcel().map(x=>(x.year,x.ProductionUS)).toMap
    val barley = barleyROW.map(x=>(x._1,x._2,barleyUS(x._1))).map(x=>(x._1,x._2,pctProdScala(x._3.toFloat,x._2.toFloat)))
    println(barley)
    
  }


  class BarleyUSExcelReader(val fileName: String,val start:Int,val end:Int) {
    val file = new File(fileName)
    val fileInputStream = new FileInputStream(file)
    val workbook = new XSSFWorkbook(fileInputStream)
    val sheet = workbook.getSheet("Barley")
    def streadExcel():IndexedSeq[BarleyUS]= {
      for {
        i <- start to end
        values = sheet.getRow(i).map(_.toString).toArray
      } yield BarleyUS(values(0), values(1),values(2),values(3))
    }
}}
