package sparkjst.cluster

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by zhaokangpan on 2016/12/6.
  */
object SparkModel {

  def prior2beta(sc : SparkContext, option: SparkJstOption, c: Coefficient): Unit = {
    val indexSentPos = sc.textFile(option.indexSentPos).map(l => {
      val p = l.split("\t")
      (p(0).toInt, p(1).toInt)
    }).collect

    for(item <- indexSentPos){
      val index = item._1
      val pos = item._2
      if(pos != -1){
        c.lambda_lw(pos)(index) = 1
      }
    }

    for(l <- 0 until option.nSentLabs){
      for(z <- 0 until option.kTopic){
        c.betaSum_lz(l)(z) = 0.0
        for(w <- 0 until option.vocabSize){
          c.beta_lzw(l)(z)(w) = c.beta_lzw(l)(z)(w) * c.lambda_lw(l)(w)
          c.betaSum_lz(l)(z) += c.beta_lzw(l)(z)(w)
        }
      }
    }
  }

  def initCoff(sc : SparkContext, option : SparkJstOption, c : Coefficient): Coefficient = {

    //处理alpha
    if (option.alpha <= 0) {
      option.alpha = option.aveDocLength * 0.05 / (option.nSentLabs * option.kTopic).toDouble
    }
    for( l <- 0 until option.nSentLabs){
      c.alphaSum_l(l) = 0.0
      for( z <- 0 until option.kTopic){
        c.alpha_lz(l)(z) = option.alpha
        c.alphaSum_l(l) += c.alpha_lz(l)(z)
      }
    }

    print(option.nSentLabs + " " + option.kTopic + " " + option.vocabSize)
    //初始化beta
    if (option.beta <= 0) option.beta = 0.01
    for( l <- 0 until option.nSentLabs){
      for( z <- 0 until option.kTopic){
        c.betaSum_lz(l)(z) = 0.0
        for( r <- 0 until option.vocabSize){
          c.beta_lzw(l)(z)(r) = option.beta
        }
      }
    }

    //初始化lamada
    for(l <- 0 until option.nSentLabs){
      for(w <- 0 until option.vocabSize){
        c.lambda_lw(l)(w) = 1.0
      }
    }

    //更新beta
    prior2beta(sc, option, c)

    //初始化gamma
    if (option.gamma <= 0 ) {
      option.gamma = option.aveDocLength * 0.05 / option.nSentLabs.toDouble
    }
    c.gammaSum_d(0) = 0.0
    for(l <- 0 until option.nSentLabs){
      c.gamma_dl(l) = option.gamma
      c.gammaSum_d(0) += c.gamma_dl(l)
    }
    c
  }

  // 启动spark集群
  def startSpark(remote: Boolean) = {
    var scMaster = ""
    if (remote) {
      scMaster = "spark://202.112.113.199:7077" // e.g. 集群
    } else {
      scMaster = "local[4]" // e.g. local[4]
    }
    val conf = new SparkConf().setAppName("SparkModel").setMaster(scMaster)
    val sparkContext = new SparkContext(conf)
    sparkContext
  }

  //重新启动spark集群
  def restartSpark(sc: SparkContext, remote: Boolean): SparkContext = {
    // After iterations, Spark will create a lot of RDDs and I only have 4g mem for it.
    // So I have to restart the Spark. The thread.sleep is for the shutting down of Akka.
    sc.stop()
    Thread.sleep(2000)
    var scMaster = ""
    if (remote) {
      scMaster = "spark://202.112.113.199:7077" // e.g. 集群
    } else {
      scMaster = "local[4]" // e.g. local[4]
    }
    val conf = new SparkConf().setAppName("SparkModel").setMaster(scMaster)
    val sparkContext = new SparkContext(conf)
    sparkContext
  }

  def updateNlzw(sentTopicTerm: List[((Int, Int, Int), Int)], option: SparkJstOption) = {
    val nlzw = Array.ofDim[Int](option.nSentLabs, option.kTopic, option.vocabSize)
    sentTopicTerm.foreach( t => {
      val sent = t._1._1
      val topic = t._1._2
      val term = t._1._3
      val count = t._2
      nlzw(sent)(topic)(term) += count
    })
    nlzw
  }

  def updateNlz(sentTopicTerm: List[((Int, Int, Int), Int)], option : SparkJstOption) = {
    val nlz = Array.ofDim[Int](option.nSentLabs, option.kTopic)
    sentTopicTerm.foreach(t => {
      val sent = t._1._1
      val topic = t._1._2
      val count = t._2
      nlz(sent)(topic) += count
    })
    nlz
  }

  def gibbsSampling(sentTopicAssignArray : Array[(Int, Int, Int)], nd : Array[Int],
                    ndl : Array[Int],
                    ndlz : Array[Array[Int]],
                    nlzw : Array[Array[Array[Int]]],
                    nlz : Array[Array[Int]],
                    option : SparkJstOption,
                    c: Coefficient) = {
    for(t <- 0 until sentTopicAssignArray.length){
      var sentLab = sentTopicAssignArray(t)._1
      var topic = sentTopicAssignArray(t)._2
      val word = sentTopicAssignArray(t)._3

      //全部减1
      nd(0) -= 1
      ndl(sentLab) -= 1
      ndlz(sentLab)(topic) -= 1
      nlzw(sentLab)(topic)(word) -= 1
      nlz(sentLab)(topic) -= 1

      //定义变量p[sent][topic]
      val p = Array.ofDim[Double](option.nSentLabs, option.kTopic)

      for( l <- 0 until option.nSentLabs){
        for( z <- 0 until option.kTopic){
          p(l)(z) = (nlzw(l)(z)(word) + c.beta_lzw(l)(z)(word))/(nlz(l)(z) + c.betaSum_lz(l)(z)) *
            (ndlz(l)(z) + c.alpha_lz(l)(z)) / (ndl(l) + c.alphaSum_l(l)) * (ndl(l) + c.gamma_dl(l)) / (nd(0) + c.gammaSum_d(0))
        }
      }

      // accumulate multinomial parameters
      for(l <- 0 until option.nSentLabs){
        for( z <- 0 until option.kTopic){
          if(z == 0){
            if(l != 0) p(l)(z) += p(l - 1)(option.kTopic - 1)
          }else{
            p(l)(z) += p(l)(z - 1)
          }
        }
      }

      // probability normalization
      val threshold = Random.nextDouble() * p(option.nSentLabs - 1)(option.kTopic - 1)

      var flag = true
      for(l <- 0 until option.nSentLabs){
        for(z <- 0 until option.kTopic){
          if(p(l)(z) > threshold && flag){
            sentLab = l
            topic = z
            flag = false
          }
        }
      }

      if (sentLab == option.nSentLabs) sentLab = option.nSentLabs - 1 // to avoid over array boundary
      if (topic == option.kTopic) topic = option.kTopic - 1

      //更新tuple值
      sentTopicAssignArray(t) = (sentLab, topic, word)

      //全部加1
      nd(0) += 1
      ndl(sentLab) += 1
      ndlz(sentLab)(topic) += 1
      nlzw(sentLab)(topic)(word) += 1
      nlz(sentLab)(topic) += 1
    }
    (sentTopicAssignArray, nd, ndl, ndlz)
  }

  def execEstimate(sc : SparkContext,
                   iterInputDocuments : RDD[(String, Array[Int], Array[Int], Array[Array[Int]], Array[(Int, Int, Int)], String)],
                   nlzw : Array[Array[Array[Int]]],
                   nlz : Array[Array[Int]],
                   option: SparkJstOption,
                   c: Coefficient): Unit ={
    var scc = sc
    var updateDocuments = iterInputDocuments
    var iterTrainDoc = iterInputDocuments
    var nlzwTmp = nlzw
    var nlzTmp = nlz
    for(iter <- 1 until option.maxIter){
      updateDocuments = iterTrainDoc.map {
        case (docId, nd, ndl, ndlz, sentTopicAssignArray, userId) =>
          //gibbs sampling
          val (newSentTopicAssignArray, new_nd, new_ndl, new_ndlz) = gibbsSampling(sentTopicAssignArray, nd, ndl, ndlz, nlzwTmp, nlzTmp, option, c)
          (docId, new_nd, new_ndl, new_ndlz, newSentTopicAssignArray, userId)
      }

      val sentTopicReduce = updateDocuments.flatMap(l => l._5).map(t => (t, 1)).reduceByKey(_+_).collect().toList

      //release resource
      iterTrainDoc.unpersist(blocking = false)
      iterTrainDoc = updateDocuments

      nlzwTmp = updateNlzw(sentTopicReduce, option)
      nlzTmp = updateNlz(sentTopicReduce, option)

      println("iteration " + iter + " finished")

      //restart spark to optimize the memory
      if (iter % option.iterFlag == 0) {
        //save RDD temporally
        var pathDocument1=""
        var pathDocument2=""
        if(option.remote){
          pathDocument1="hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/temp/gibbsLDAtmp_final_" + option.kTopic + "_" + iter
          pathDocument2="hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/temp/gibbsLDAtmp2_final_" + option.kTopic + "_" + iter
        }else{
          pathDocument1="gibbsLDAtmp_" + iter
          pathDocument2="gibbsLDAtmp2_" + iter
        }
        val storedDocuments1=iterTrainDoc
        storedDocuments1.persist(StorageLevel.DISK_ONLY)
        storedDocuments1.saveAsObjectFile(pathDocument1)
        val storedDocuments2=updateDocuments
        storedDocuments2.persist(StorageLevel.DISK_ONLY)
        storedDocuments2.saveAsObjectFile(pathDocument2)

        //restart Spark to solve the memory leak problem
        scc = restartSpark(scc, option.remote)
        //as the restart of Spark, all of RDD are cleared
        //we need to read files in order to rebuild RDD
        iterTrainDoc = scc.objectFile(pathDocument1)
        updateDocuments = scc.objectFile(pathDocument2)

      }
    }
    //save result
    val resultDocuments = iterTrainDoc
    saveResult(scc, resultDocuments, nlzw, nlz, option, c)
  }

  def saveResult(sc : SparkContext,
                 resultDocuments : RDD[(String, Array[Int], Array[Int], Array[Array[Int]], Array[(Int, Int, Int)], String)],
                 nlzw : Array[Array[Array[Int]]],
                 nlz : Array[Array[Int]],
                 option: SparkJstOption,
                 c: Coefficient){

    //move to memory
    resultDocuments.cache()

    //save pi
    resultDocuments.map(doc => {
      val pi_dl = new Array[Double](option.nSentLabs)
      for(l <- 0 until option.nSentLabs){
        pi_dl(l) = (doc._3(l) + c.gamma_dl(l))/(doc._2(0) + c.gammaSum_d(0))
      }
      (doc._1, pi_dl)
    }).map(l => {
      var str = l._1
      for(t <- l._2){
        str += "\t" + t
      }
      str
    }).saveAsTextFile(option.piOutput)

    //save theta
    resultDocuments.flatMap( doc => {
      val result = new Array[(String, Int, Array[Double])](option.nSentLabs)
      for(l <- 0 until option.nSentLabs){
        val theta_dlz = new Array[Double](option.kTopic)
        for(z <- 0 until option.kTopic){
          theta_dlz(z) = (doc._4(l)(z) + c.alpha_lz(l)(z))/(doc._3(l) + c.alphaSum_l(l))
        }
        result(l) = (doc._1, l, theta_dlz)
      }
      result
    }).map(line => {
      var str = line._1 + "\t" + line._2
      for(t <- line._3){
        str += "\t" + t
      }
      str
    }).saveAsTextFile(option.thetaOutput)

    //save phi
    val phi_lzw = new ArrayBuffer[(Int, Int, Int, Double)]
    for(l <- 0 until option.nSentLabs){
      for( z <- 0 until option.kTopic){
        var wordSentTopic = 0.0
        for( w <- 0 until option.vocabSize){
          wordSentTopic = (nlzw(l)(z)(w) + c.beta_lzw(l)(z)(w)) / (nlz(l)(z) + c.betaSum_lz(l)(z))
          if(wordSentTopic > 0.0001){
            phi_lzw.+=((l, z, w, wordSentTopic))
          }
        }
      }
    }
    sc.parallelize(phi_lzw).map(line => {
      val str = line._1 + "\t" + line._2 + "\t" + line._3 + "\t" + line._4
      str
    }).saveAsTextFile(option.phiOutput)

  }


  def initEstimate(option: SparkJstOption, c: Coefficient): Unit ={

    //start spark
    System.setProperty("file.encoding", "UTF-8")
    val sc = startSpark(option.remote)

    //init coeffcient
    val newCoeff = initCoff(sc, option, c)

    //read training data
    //读取训练数据
    val trainDoc = sc.textFile(option.numerTrainFile).map(line => {
      val p = line.split("\t")
      val z = new Array[Int](p.length - 2)
      val l = new Array[Int](p.length - 2)
      var ll = 0
      var topic = 0
      val nd = Array[Int](1)
      nd(0) = 0
      val ndl = new Array[Int](option.nSentLabs)
      for(i <- 0 until option.nSentLabs) {ndl(i) = 0}
      val ndlz = Array.ofDim[Int](option.nSentLabs, option.kTopic)
      for(i <- 0 until option.nSentLabs){
        for(j <- 0 until option.kTopic){
          ndlz(i)(j) = 0
        }
      }
      val sentTopicAssignArray = new Array[(Int, Int, Int)](p.length - 2)//(sent, topic, word)
      for(i <- 2 until p.length){
        val item = p(i).split(" ")
        if(item(1).toInt > -1 && item(1).toInt < option.nSentLabs){
          ll = item(1).toInt
        }else{
          ll = Random.nextInt(option.nSentLabs)
        }
        l(i - 2) = ll
        topic = Random.nextInt(option.kTopic)
        z(i - 2) = topic
        nd(0) += 1
        ndl(ll) += 1
        ndlz(ll)(topic) += 1
        sentTopicAssignArray(i - 2) = (ll, topic, item(0).toInt)
      }
      (p(1), nd, ndl, ndlz, sentTopicAssignArray, p(0))
    }).cache()

    //calculate nlzw, nlz
    val sentTopicReduce = trainDoc.flatMap(l => l._5).map(t => (t, 1)).reduceByKey(_+_).collect().toList
    val nlzw = updateNlzw(sentTopicReduce, option)
    val nlz = updateNlz(sentTopicReduce, option)

    val iterInputDocuments = trainDoc

    //release resource
    trainDoc.unpersist(blocking = false)

    execEstimate(sc, iterInputDocuments, nlzw, nlz, option, newCoeff)

  }

  //coeffecient class
  case class Coefficient(alpha_lz : Array[Array[Double]],
                         alphaSum_l : Array[Double],
                         beta_lzw : Array[Array[Array[Double]]],
                         betaSum_lz : Array[Array[Double]],
                         lambda_lw : Array[Array[Double]],
                         gamma_dl : Array[Double],
                         gammaSum_d : Array[Double])


  def main(args : Array[String]): Unit ={

    val option = new SparkJstOption

    val alpha_lz = Array.ofDim[Double](option.nSentLabs, option.kTopic)

    val alphaSum_l = new Array[Double](option.nSentLabs)

    val beta_lzw = Array.ofDim[Double](option.nSentLabs, option.kTopic, option.vocabSize)

    val betaSum_lz = Array.ofDim[Double](option.nSentLabs, option.kTopic)

    val lambda_lw = Array.ofDim[Double](option.nSentLabs, option.vocabSize)

    val gamma_dl = new Array[Double](option.nSentLabs)

    val gammaSum_d = new Array[Double](1)

    val coefficient = Coefficient(alpha_lz, alphaSum_l, beta_lzw, betaSum_lz, lambda_lw, gamma_dl, gammaSum_d)

    //参数注入
    option.maxIter = args(0).toInt
    option.kTopic = args(1).toInt

    //program start
    initEstimate(option, coefficient)
  }
}