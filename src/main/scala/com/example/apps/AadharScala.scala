package com.example.apps
import com.example.common.AppVariables._
import com.example.common.Schemas._
import org.apache.log4j.{Level, Logger}
import scala.io.Source

/* Problem2: Question1 (using plain Scala) */

object AadharScala {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val aadhar = new AadharCSVReader(auth)
    val stat = new AadharStatisticsComputer(aadhar)
    val result = stat.aadharD
    for (i<-0 until 10){
      println(result(i))
    }
  }




  class AadharCSVReader(val fileName: String) extends AadharReader {

    override def readAadhar(): Seq[aadhardtl] = {
      for {
        line <- Source.fromFile(fileName).getLines().drop(1).toVector
        values = line.split(",").map(_.trim)
      } yield aadhardtl(values(2), values(3), values(128))
    }}

    class AadharStatisticsComputer(val aadharReader: AadharReader) {

      val aadharData = aadharReader.readAadhar
      val aadharD = aadharData.filter(p=>p.sa.matches("^[0-9]*$") && p.aua>"65000"&& p.res_state!=("Delhi"))

  }

}
