package sparkjst.local

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhaokangpan on 2016/12/7.
  */
object SparkPerplexityLocal {

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

  def calculatePerplexity(option: SparkJstLocalOption): Double ={
    val sc = startSpark(option.remote)

    var pathPi = option.piOutput
    var pathTheta = option.thetaOutput
    var pathPhi = option.phiOutput

    //读取phi
    val phi = sc.textFile(pathPhi).map(l => {
      val p = l.split("\t")
      ((p(1), p(0)), (p(2), p(3).toDouble))
    }).collectAsMap//(tid, sid), (index, value))
    val phiMap = sc.broadcast(phi)

    //读取theta
    val theta = sc.textFile(pathTheta).map(l => {
      val p = l.split("\t")
      ((p(3), p(2)), (p(0), p(4).toDouble))
    }).filter( l => l._2._2 > 0.0)//((tid, sid), (wid, value))

    val phi_theta = theta.mapPartitions(iter => {
      val phi = phiMap.value
      for{
        (key, value) <- iter
        if(phi.contains(key))
      } yield ((key._2, value._1, phi.get(key).getOrElse(("", 0.0))._1), phi.get(key).getOrElse(("", 0.0))._2 * value._2)
    }).filter(l => !l._1._3.equals("")).reduceByKey(_+_).map(l => ((l._1._1, l._1._2), (l._1._3, l._2)))//((sid, wid), (index, value))
    phi_theta.collect.foreach(l => println(l))

    //读取pi
    val pi = sc.textFile(pathPi).flatMap(l => {
      val p = l.split("\t")
      for(i <- 2 until p.length) yield (((i-2).toString, p(0)), p(i).toDouble)
    }).collectAsMap//(sid, wid), value)
    val piMap = sc.broadcast(pi)

    val pi_phi_theta = phi_theta.mapPartitions(iter => {
      val pi = piMap.value
      for{
        (key, value) <- iter
        if(pi.contains(key))
      } yield ((key._2, value._1), pi.get(key).getOrElse(0.0) * value._2)
    }).reduceByKey(_+_) //((wid, index), value)

    pi_phi_theta.collect.foreach(l => println(l))

    val temp = pi_phi_theta.map(l => l._2).sum()
    println("sum : " + temp)

    val res = Math.exp(- pi_phi_theta.mapValues( l => Math.log(l) ).map(l => l._2).sum()/option.corpusSize)

    res
  }

  def main(args : Array[String]): Unit ={
    val option = new SparkJstLocalOption()
    val perplexity = calculatePerplexity(option)
    println("perplexity : " + perplexity)
  }
}
