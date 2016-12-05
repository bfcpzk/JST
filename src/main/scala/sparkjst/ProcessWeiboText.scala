package sparkjst

import java.io.{PrintWriter, File}

import com.hankcs.hanlp.dictionary.CustomDictionary
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
  * Created by zhaokangpan on 2016/12/5.
  */
object ProcessWeiboText {

  def main(args : Array[String]): Unit ={
    val writer = new PrintWriter(new File("weibodata/weiboDivideHigh.txt"))

    val fs = Source.fromFile("data/senti.txt").getLines().map(l => {
      val p = l.split("\t")
      p(0).trim
    }).foreach(l => CustomDictionary.add(l, "nz 1024 n 1"))//avoid dividing wrongly

    /*val file = Source.fromFile("weibodata/fweibo_part1.txt").getLines().filter(l => l.split("\t").length == 8).map(l => {
      val p = l.split("\t")
      val segtext = StandardTokenizer.segment(p(4).toArray)
      var res = ""
      for(i <- segtext){
        println(i)
      }
      (p(0), p(1), p(3), res)
    }).map(l => l._1 + "\t" + l._2 + "\t" + l._3 + "\t" + l._4 + "\t").foreach(l => writer.write(l))
    writer.close()*/
  }
}
