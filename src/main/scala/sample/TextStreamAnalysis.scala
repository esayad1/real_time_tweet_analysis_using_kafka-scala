
package sample
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TextStreamAnalysis {
  def main(args: Array[String]) {
    // Set the log level to WARN
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("Text Stream Analysis")
      .master("local[*]")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10)) // Change the batch interval to 10 seconds
    ssc.checkpoint("/Users/benadem/Documents/checkpoint-dir")

    val fileStream = ssc.textFileStream("/Users/benadem/Documents/streaming-dir")

    val words = fileStream.flatMap(_.split(" "))

    val hashtags = words.filter(_.startsWith("#"))
    val hashtagCounts = hashtags.map(hashtag => (hashtag, 1))

    // Function to update the count of hashtags
    val updateCount = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    // Use updateStateByKey to maintain cumulative count
    val cumulativeCounts = hashtagCounts.updateStateByKey(updateCount)

    // Sort by hashtag count and get the top 10
    val sortedCounts = cumulativeCounts.transform(rdd => rdd.sortBy(_._2, ascending = false))

    // Print the top 10 hashtags in a more readable format
    sortedCounts.foreachRDD { rdd =>
      println("\nTop 10 hashtags:")
      for ((tag, count) <- rdd.take(10)) {
        println(s"$tag: $count")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
