package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Created by zk_chs on 16/8/13.
  * spark-submit --class "spark.WordCount"  /root/spark-jars/bigdata-spark-1.0-SNAPSHOT.jar
  */
object WordCount {

  def main(args: Array[String]) {
    val logFile = "hdfs://Master:9000/user/root/input/spark/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("WordCount Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

}