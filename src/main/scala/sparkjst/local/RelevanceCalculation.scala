package sparkjst.local

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 2017/1/2.
  */
object RelevanceCalculation {

  def main(args : Array[String]): Unit ={
    val conf = new SparkConf().setAppName("relevanceCalculation").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val option = new SparkJstLocalOption

    /*
     * 第一项
     */
    val pML = sc.textFile(option.pML).map(l => {
      val p = l.split("\t")
      ((p(0), p(1), p(2)), p(3).toDouble)
    }).cache()//((senti, topic, index), value)

    /*
     * 第二项
     */
    val dict = sc.textFile(option.wordMap).map(l => {
      val p = l.split("\t")
      (p(0), p(1))
    })//(wordC, index)

    val wordPmltm = sc.textFile(option.trainFile)
      .filter(l => l.split("/t").length == 4)
      .flatMap(l => {
      val p = l.split("\t")
      val file = p(3)
      val wordList = file.split(" ")
      for(term <- wordList) yield ((p(2), term), 1)
    }).distinct()
      .map(l => (l._1._2, 1))
      .reduceByKey(_+_)
      .mapValues(l => l * 1.0/option.numDocs)//(wordC, pmltm)

    val pmltm = wordPmltm.join(dict).map(l => {
      (l._2._2, l._2._1)
    })//(index, pmltm)

    /*
     * 第三项
     */
    val phi = sc.textFile(option.phiOutput).map(l => {
      val p = l.split("\t")
      ((p(0), p(1), p(2)), p(3).toDouble)
    })//((senti, topic, index), value)

    /*
     * 设置参数
     */
    val lamda = 0.3
    val eta = 0.5

    val one = (1 - lamda) * eta
    val two = eta * lamda
    val three = 1 - eta

    //计算中间结果(1, 3)连接
    val ptM = pML.mapValues(l => l * one)
      .join(phi.mapValues(l => l * three))
      .map(l => {
      (l._1, l._2._1 + l._2._2)
    }).map(l => (l._1._3, (l._1, l._2)))
      .join(pmltm.mapValues(l => l * two))
      .map(l => (l._2._1._1._3, (l._2._1._1._1, l._2._1._1._2, l._2._1._2 + l._2._2)))
    //(index, (senti, topic, value))

    phi.unpersist(blocking = false)
    pmltm.unpersist(blocking = false)
    pML.unpersist(blocking = false)

    val queryExpansion = sc.textFile(option.queryExpansion).collect().toSet

    //中间结果储存
    val pqlk =  ptM.filter(l => queryExpansion.contains(l._1))
      .map(l => ((l._2._1, l._2._2), l._2._3))
      .reduceByKey(_*_)//((senti, topic), value)

    //读取theta
    val theta = sc.textFile(option.thetaOutput).map(l => {
      val p = l.split("\t")
      ((p(2), p(3)), (p(1), p(4).toDouble))
    })//((senti, topic), (userid, value))

    //最终聚合
    theta.join(pqlk)
      .map(l => ((l._2._1._1, l._1._1, l._1._2), l._2._1._2 * l._2._2))
      .reduceByKey(_+_)//((userid, senti, topic),value)
      .saveAsTextFile(option.relevanceOutput)
  }
}
