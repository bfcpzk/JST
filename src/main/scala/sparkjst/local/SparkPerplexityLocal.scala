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

    //读取phi
    val phi = sc.textFile(option.phiOutput).map(l => {
      val p = l.split("\t")
      ((p(0), p(1)),(p(2), p(3).toDouble))
    }).collectAsMap//((sid, tid),(index, value))
    var phiMap = sc.broadcast(phi)

    //读取pi
    val pi = sc.textFile(option.piOutput).flatMap(l => {
      val p = l.split("\t")
      for(i <- 2 until p.length) yield ((p(0), (i-2).toString), p(i).toDouble)
    })//((wid, sid), value)


    //读取theta
    val theta = sc.textFile(option.thetaOutput).map(l => {
      val p = l.split("\t")
      ((p(2), p(3)), (p(0), p(4).toDouble))
    })//((sid, tid), (wid, value))

    val phi_theta = theta.mapPartitions(iter => {
      val phi = phiMap.value
      for{
        (key, value) <- iter
        if(phi.contains(key))
      } yield ((value._1, key._1), (phi.get(key).getOrElse(("", 0.0))._1, phi.get(key).getOrElse(("",0.0))._2 * value._2))
    }).filter(l => !l._2._1.equals("")).collectAsMap//((wid, sid), (index, value))
    var tmp_phi_theta = sc.broadcast(phi_theta)

    val pi_theta_phi = pi.mapPartitions( iter => {
      val phiTheta = tmp_phi_theta.value
      for{
        (key, value) <- iter
        if(phiTheta.contains(key))
      } yield ((key._1, phiTheta.get(key).getOrElse(("", 0.0))._1), phiTheta.get(key).getOrElse(("", 0.0))._2 * value)
    }).filter(l => l._2 > 0.0).reduceByKey(_+_)//((wid, index), log(value)) 对相同的(sid, tid)聚合相加

    val temp = pi_theta_phi.map(l => l._2).sum()
    println(pi_theta_phi.count())
    println(temp)

    val res = pi_theta_phi.mapValues( l => Math.log(l)).map(l => l._2).sum()/option.numDocs

    //pi.unpersist(blocking = false)
    //theta.unpersist(blocking = false)
    //val res = pi_theta_phi.map(l => l._2).sum()/option.numDocs

    res
  }

  def main(args : Array[String]): Unit ={
    val option = new SparkJstLocalOption
    val perplexity = calculatePerplexity(option)
    println(perplexity)
  }
}
