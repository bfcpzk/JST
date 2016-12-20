package sparkjst.cluster

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaokangpan on 2016/12/7.
  */
object SparkPerplexity {

  // 启动spark集群
  def startSpark(option : SparkJstHongKongOption) = {
    var scMaster = ""
    if (option.remote) {
      scMaster = option.scMaster // e.g. 集群
    } else {
      scMaster = "local[4]" // e.g. local[4]
    }
    val conf = new SparkConf().setAppName("SparkPerplexity").setMaster(scMaster)
    val sparkContext = new SparkContext(conf)
    sparkContext
  }

  case class TT(kTopic : Int, kTable : Int)

  def calculatePerplexity(option: SparkJstHongKongOption, rate : Double, iter : Int): Double ={
    val sc = startSpark(option)

    var pathPi = ""
    var pathTheta = ""
    var pathPhi = ""
    if(iter % 5 == 0){
      pathPi = option.piOutput + "_" + iter
      pathTheta = option.thetaOutput + "_" + iter
      pathPhi = option.phiOutput + "_" + iter
    }else{
      pathPi = option.piOutput
      pathTheta = option.thetaOutput
      pathPhi = option.phiOutput
    }

    //读取phi
    val phi = sc.textFile(pathPhi).map(l => {
      val p = l.split("\t")
      (p(0),(p(1), p(2), p(3).toDouble))
    }).collectAsMap//(sid, (tid, index, value))
    var phiMap = sc.broadcast(phi)

    //读取pi
    val pi = sc.textFile(pathPi).flatMap(l => {
      val p = l.split("\t")
      for(i <- 2 until p.length) yield ((i-2).toString, (p(0), p(i).toDouble))
    })//(sid, (wid, value))

    val phi_pi = pi.mapPartitions(iter => {
      val phi = phiMap.value
      for{
        (key, value) <- iter
        if(phi.contains(key))
      } yield ((key, phi.get(key).getOrElse(("", "", 0.0))._1, value._1), (phi.get(key).getOrElse(("", "", 0.0))._2, phi.get(key).getOrElse(("", "", 0.0))._3 * value._2))
    }).filter(l => (!l._2._1.equals("") && !l._1._2.equals("")))
      .collectAsMap//((sid, tid, wid), (index, value))

    val tmp_phi_pi = sc.broadcast(phi_pi)

    //读取theta
    val theta = sc.textFile(pathTheta).map(l => {
      val p = l.split("\t")
      ((p(2), p(3), p(0)), p(4).toDouble)
    }).filter( l => l._2 > rate)//((sid, tid, wid), value)

    //全连接
    val phi_pi_theta = theta.mapPartitions( iter => {
      val phiPi = tmp_phi_pi.value
      for{
        (key, value) <- iter
        if(phiPi.contains(key))
      } yield ((key._3, phiPi.get(key).getOrElse(("", 0.0))._1), phiPi.get(key).getOrElse(("", 0.0))._2 * value)
    }).filter(l => !l._1._2.equals("")).reduceByKey(_+_).filter(l => l._2 > 0.0)//((wid, index), value) 对相同的(sid, tid)聚合相加

    val temp = phi_pi_theta.map(l => l._2).sum()
    println(temp)

    val res = Math.exp(- phi_pi_theta.mapValues( l => Math.log(l)).map(l => l._2).sum()/option.numDocs)

    res
  }

  def main(args : Array[String]): Unit ={
    val option = new SparkJstHongKongOption
    val rate = args(0).toDouble
    val iter = args(1).toInt
    val perplexity = calculatePerplexity(option, rate, iter)
    println("iter : " + iter)
    println("perplexity : " + perplexity)
  }
}
