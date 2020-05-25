package com.example.apps
import java.io.File
import com.example.common.AppVariables._
import com.example.common.Schemas._
import scala.collection.JavaConversions._
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import java.io.FileInputStream
import org.apache.poi.xssf.usermodel.XSSFWorkbook



object excelReader {
  def main(args: Array[String]): Unit = {
    val poverty = new PovertyExcelReader(povertyEstimates,6,100).readExcel()
    val statesrow = new StatesExcelReader(states,1,50).streadExcel()
    val stateMap:Map[String,String]=statesrow.map(x=>(x.State_ab,x.State_name)).toMap
   val povertytab = poverty.filter(x=>(!x.Urban_Influence_Code_2003.isEmpty && !x.Rural_urban_Continuum_Code_2013.isEmpty))
     .filter(x=>x.Urban_Influence_Code_2003.toFloat%2==1 && x.Rural_urban_Continuum_Code_2013.toFloat%2==0)
       .map(x=>(stateMap(x.State),x.Area_name.concat(" "+x.State),x.Urban_Influence_Code_2003,x.Rural_urban_Continuum_Code_2013,over17Normal(x.PCTPOVALL_2018.toFloat,x.PCTPOV017_2018.toFloat)))
  println(povertytab)
  }

  class PovertyExcelReader(val fileName: String,val start:Int,val end:Int) extends ExcelReader {
    val file = new File(fileName)
    val fileInputStream = new FileInputStream(file)
    val workbook = new HSSFWorkbook(fileInputStream)
    val sheet = workbook.getSheet("Poverty Data 2018")
    override def readExcel():IndexedSeq[poverty]= {
      for {
        i<-start to end
        values = sheet.getRow(i).map(_.toString).toArray
      } yield poverty(values(1),values(2),values(4),values(5),values(10),values(16))
    }}

  class StatesExcelReader(val fileName: String,val start:Int,val end:Int) {
    val file = new File(fileName)
    val fileInputStream = new FileInputStream(file)
    val workbook = new XSSFWorkbook(fileInputStream)
    val sheet = workbook.getSheet("Sheet2")
    def streadExcel():IndexedSeq[statesinfo]= {
      for {
        i <- start to end
        values = sheet.getRow(i).map(_.toString).toArray
      } yield statesinfo(values(1), values(0))
    }}
}