package sparkjst.process

import java.util

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer


/**
  * Created by zhaokangpan on 2016/12/21.
  * filter the low frequency words of each weibo
  */
object FileWordFilter {
  def main(args : Array[String]): Unit ={

    val conf = new SparkConf().setAppName("FileWordFilter").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val fileWordCount = sc.textFile("newDivide.txt").filter(l => l.split("\t").length == 4).flatMap(l => {
      val p = l.split("\t")(3).split(" ")
      for(item <- p) yield (item, 1)
    }).reduceByKey(_+_).collect

    val file = new ArrayBuffer[String]()

    for(t <- fileWordCount){
      if(t._2 <= 2 || t._2 >= 100000){
        file.+=(t._1)
        //println(t._1)
      }
    }

    sc.parallelize(file).saveAsTextFile("filterWord")

    //fileWordCount.map(l => (l._2, 1)).reduceByKey(_+_).sortBy(l => l._1)

  }
}
