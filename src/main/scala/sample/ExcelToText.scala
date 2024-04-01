package sample

import org.apache.log4j.{Level, Logger}
import org.apache.poi.ss.usermodel._
import org.apache.poi.xssf.usermodel._

import java.io._
import java.util.{Timer, TimerTask}
import scala.collection.JavaConverters._

object ExcelToText {
  def main(args: Array[String]) {

    // Set the log level to WARN
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val workbook: Workbook = WorkbookFactory.create(new File("/Users/benadem/Documents/dataset_free-tiktok-scraper_2022-07-27_21-44-20-266.xlsx"))
    val sheet: Sheet = workbook.getSheetAt(0)
    val rows: Iterator[Row] = sheet.rowIterator().asScala

    val dir = new File("/Users/benadem/Documents/streaming-dir")
    if (!dir.exists()) {
      dir.mkdir()
    }

    val timer = new Timer()
    var count = 0

    timer.schedule(new TimerTask {
      override def run() {
        val file = new File(s"/Users/benadem/Documents/streaming-dir/output_$count.txt")
        val writer = new PrintWriter(new FileOutputStream(file, true)) // append to file
        rows.take(10).foreach(row => {
          val cell = row.getCell(47)
          if (cell != null) {
            writer.write(cell.toString + "\n")
          }
        })
        writer.close()
        count += 1
      }
    }, 0, 10000) // 10 seconds
  }
}
