import java.io._
import scala.io.StdIn.readLine

object KeyboardInput {
  def main(args: Array[String]): Unit = {
    val directoryPath = "/Users/benadem/Documents/streaming-dir" // replace with your directory path
    while (true) {
      println("Enter something: ")
      val input = readLine()
      val pw = new PrintWriter(new File(s"$directoryPath/file_${System.currentTimeMillis}.txt" ))
      pw.write(input)
      pw.close
    }
  }
}
