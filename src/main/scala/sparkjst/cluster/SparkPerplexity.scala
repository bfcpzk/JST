package sparkjst.cluster

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaokangpan on 2016/12/7.
  */
object SparkPerplexity {

  // 启动spark集群
  def startSpark(remote: Boolean) = {
    var scMaster = ""
    if (remote) {
      scMaster = "spark://202.112.113.199:7077" // e.g. 集群
    } else {
      scMaster = "local[4]" // e.g. local[4]
    }
    val conf = new SparkConf().setAppName("SparkPerplexityLocal").setMaster(scMaster)
    val sparkContext = new SparkContext(conf)
    sparkContext
  }

  def calculatePerplexity(option: SparkJstOption): Double ={
    val sc = startSpark(option.remote)

    //读取pi
    val pi = sc.textFile(option.piOutput).flatMap(l => {
      val p = l.split("\t")
      for(i <- 2 until p.length) yield ((p(0), (i-2).toString), p(i).toDouble)
    })//((wid, sid), value)

    //读取theta
    val theta = sc.textFile(option.thetaOutput).flatMap(l => {
      val p = l.split("\t")
      for(i <- 3 until p.length) yield ((p(0), p(2)), ((i-3).toString, p(i).toDouble))
    })//((wid, sid), (tid, value))

    val tmp_theta_pi = theta.leftOuterJoin(pi).map(l => (l._1, (l._2._1._1, l._2._1._2 * l._2._2.getOrElse(0.0))))
      .map(l => ((l._1._2, l._2._1), (l._1._1, l._2._2))).cache()//((sid, tid),(wid, value))

    pi.unpersist(blocking = false)
    theta.unpersist(blocking = false)

    //读取phi
    val phi = sc.textFile(option.phiOutput).map(l => {
      val p = l.split("\t")
      ((p(0), p(1)),(p(2), p(3).toDouble))
    })//((sid,tid),(index, value))

    val pi_theta_phi = tmp_theta_pi.leftOuterJoin(phi)
      .map(l => ((l._2._1._1, l._2._2.getOrElse(("default", 0.0))._1), l._2._2.getOrElse(("default", 0.0))._2 * l._2._1._2))
      .reduceByKey(_+_).mapValues( l => Math.log(l))//((wid, index), log(value)) 对相同的(sid, tid)聚合相加

    phi.unpersist(blocking = false)
    tmp_theta_pi.unpersist(blocking = false)

    val res = pi_theta_phi.map(l => l._2).sum()/option.numDocs

    res
  }

  def main(args : Array[String]): Unit ={
    val option = new SparkJstOption
    val perplexity = calculatePerplexity(option)
    println(perplexity)
  }
}
