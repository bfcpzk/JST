package sparkjst

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * Created by zhaokangpan on 2016/11/21.
  */
object SparkMain {


  def main(args : Array[String]): Unit ={


    //estimate
    val model = new SparkModel
    //model.initOptionParameter(opt)
    model.initEstimate()
  }
}
