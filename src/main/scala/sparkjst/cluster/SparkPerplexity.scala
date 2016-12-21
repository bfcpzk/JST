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
      ((p(1), p(0)), (p(2), p(3).toDouble))
    }).collectAsMap//(tid, sid), (index, value))
    val phiMap = sc.broadcast(phi)

    //读取theta
    val theta = sc.textFile(pathTheta).map(l => {
      val p = l.split("\t")
      ((p(3), p(2)), (p(0), p(4).toDouble))
    }).filter( l => l._2._2 > rate)//((tid, sid), (wid, value))

    val phi_theta = theta.mapPartitions(iter => {
      val phi = phiMap.value
      for{
        (key, value) <- iter
        if(phi.contains(key))
      } yield ((key._2, value._1, phi.get(key).getOrElse(("", 0.0))._1), phi.get(key).getOrElse(("", 0.0))._2 * value._2)
    }).filter(l => !l._1._3.equals("") && l._2 > 0.0).reduceByKey(_+_).map(l => ((l._1._1, l._1._2), (l._1._3, l._2)))//((sid, wid), (index, value))

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
    }).reduceByKey(_+_).filter(l => l._2 > 0.0) //((wid, index), value)

    val temp = pi_phi_theta.mapValues( l => Math.log(l) ).map(l => l._2).sum()
    println("指数分子：" + temp)
    println("语料集单词数：" + option.corpusSize)
    println("指数：" + temp * 1.0/option.corpusSize)

    val res = Math.exp(- temp * 1.0/option.corpusSize)

    res
  }

  def main(args : Array[String]): Unit = {
    val option = new SparkJstHongKongOption
    val rate = args(0).toDouble
    val iter = args(1).toInt
    val perplexity = calculatePerplexity(option, rate, iter)
    println("iter : " + iter)
    println("perplexity : " + perplexity)
  }
}
