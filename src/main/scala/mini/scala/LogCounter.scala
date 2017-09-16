package mini.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object LogCounter {
    def main(args: Array[String]) {
    
        val logfile = args(0)

        val conf = new SparkConf().setMaster("local").setAppName("logs")
        
        val sc = new SparkContext(conf)
        
        val logs = sc.textFile(logfile)
        
        val badlogs = logs.filter(line => line.contains("WARN") || line.contains("ERROR"))
        
        println("Input had " + badlogs.count() + " bad logs")
    }
}
    