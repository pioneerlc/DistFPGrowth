import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer
import scala.compat.Platform._
import scala.util.control.Breaks

/**
 * Created by clin3 on 2014/12/24.
 */
object DistFPGrowthTest {
  def main(args: Array[String]): Unit = {
    val supportThreshold = args(0).toDouble
    val confidenceThreshold = args(1).toDouble
    val liftThreshold = args(2).toDouble
    val fileName = args(3)

    //Initialize SparkConf.
    val conf = new SparkConf()
    conf.setAppName("FPGrowth").set("spark.cores.max", "192").set("spark.executor.memory", "160G")

    //Initialize SparkContext.
    val sc = new SparkContext(conf)

    //Create distributed datasets from hdfs.
    val input = sc.textFile("hdfs://sr471:54311/user/clin/fpgrowth/input/" + fileName, 2)


    val rdd = DistFPGrowth.run(input, supportThreshold, " ", 1)

    val count = rdd.count()

    val localRDD = rdd.collect()

    for(i <- localRDD) {
      println(i._1 + " " + i._2)
    }

    val startTime = currentTime
    val ret = new ArrayBuffer[((String, String), (Double, Double))]()
    for(i <- 0 until(localRDD.length - 1)) {
      for(j <- (i + 1) until(localRDD.length)) {
        if(localRDD(i)._1.length < localRDD(j)._1.length) {
          val x = localRDD(i)._2
          val y = localRDD(j)._2
          val conf = y.toDouble / x
          if(conf >= confidenceThreshold) {
            val rule = genRule(localRDD(j)._1, localRDD(i)._1)
            rule match {
              case Some(x) => {
                val lift = computeLift(x, localRDD, conf)
                if(lift >= liftThreshold) {
                  val tuple = (x, (conf, lift))
                  ret += tuple
                }
              }
              case None =>
            }
          }
        } else if(localRDD(i)._1.length > localRDD(j)._1.length) {
          val x = localRDD(i)._2
          val y = localRDD(j)._2
          val conf = x.toDouble / y
          if(conf >= confidenceThreshold) {
            val rule = genRule(localRDD(i)._1, localRDD(j)._1)
            rule match {
              case Some(x) => {
                val lift = computeLift(x, localRDD, conf)
                if(lift >= liftThreshold) {
                  val tuple = (x, (conf, lift))
                  ret += tuple
                }
              }
              case None =>
            }
          }
        }
      }
    }

    def genRule(first: String, second: String): Option[(String, String)] = {
      val left = new ArrayBuffer[String]()
      val right = new ArrayBuffer[String]()
      var x = first.split(" ").toSet
      val y = second.split(" ")
      var isBreak = false
      val loop = new Breaks()
      loop.breakable(
        for(i <- y) {
          if(x.contains(i)) {
            left += i
            x -= i
          } else {
            isBreak = true
            loop.break()
          }
        }
      )
      if(isBreak == false) {
        for(i <- x) {
          right += i
        }
        Some(left.mkString(" "), right.sortWith(_ < _).mkString(" "))
      } else {
        None
      }
    }

    def computeLift(rule: (String, String), localRDD: Array[(String, Long)], conf: Double): Double = {
      val map = localRDD.toMap
      var lift = Double.MinValue
      map.get(rule._2) match {
        case Some(x) => {
          //println("x = " + x + " NumberOfDB = " + DistFPGrowth.NumberOfDB)
          lift = conf / (x.toDouble / DistFPGrowth.NumberOfDB)
        }
        case None =>
      }
      lift
    }


    for(i <- ret) {
      print("lhs = { ")
      print(i._1._1)
      print(" } rhs = { ")
      print(i._1._2)
      print(" } ")
      print(" conf = %.2f".format(i._2._1))
      println(" lift = %.2f".format(i._2._2))
    }

    val endTime = currentTime
    val totalTime: Double = endTime - startTime
    println("---------------------------------------------------------")
    println("This program totally took " + totalTime/1000 + " seconds.")
    println("---------------------------------------------------------")
    println("Length of freqList = " + DistFPGrowth.LengthOfFreqList)
    println("Number of frequent itemsets = " + count)
    println("Number of rules = " + ret.length)

    //Stop SparkContext.
    sc.stop()
  }
}
