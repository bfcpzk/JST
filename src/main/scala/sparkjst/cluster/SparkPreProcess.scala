package sparkjst.cluster

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * Created by zhaokangpan on 2016/12/6.
  */
object SparkPreProcess {
  //start spark
  def startSpark(remote: Boolean) = {
    var scMaster = ""
    if (remote) {
      scMaster = "spark://202.112.113.199:7077" // e.g. 集群
    } else {
      scMaster = "local[4]" // e.g. local[4]
    }
    val conf = new SparkConf().setAppName("SparkPreProcess").setMaster(scMaster)
    val sparkContext = new SparkContext(conf)
    sparkContext
  }

  //save (word, index) dictionary
  def saveWordIndexMap( sc: SparkContext, wordMap : HashMap[String, Int], option: SparkJstOption): Unit ={
    val wordArray = new ArrayBuffer[String]()
    for(item <- wordMap){
      wordArray.+=(item._2 + " " + item._1)
    }
    sc.parallelize(wordArray).saveAsTextFile(option.wordMap)
  }

  //process
  def jstFileProcess(option : SparkJstOption){
    //start spark
    System.setProperty("file.encoding", "UTF-8")
    val sc = startSpark(option.remote)

    //read sentiment dictionary and training corpus
    val trainFile = sc.textFile(option.trainFile).cache()
    val docSize = trainFile.count.toInt
    val corpusSize = trainFile.filter(l => l.split("\t").length == 4).flatMap(l => {
      val p = l.split("\t")(3).split(" ")
      for(i <- 0 until p.length) yield ("word", 1)
    }).reduceByKey(_+_).map(l => l._2).collect()(0)

    val allWords = trainFile.filter(l => l.split("\t").length == 4).flatMap(l => {
      val p = l.split("\t")(3).split(" ")
      for(i <- 0 until p.length) yield p(i)
    }).distinct.collect().toList.sortWith(_ < _)

    //calculate the parameters
    val avgDocSize = (corpusSize * 1.0)/docSize
    val vSize = allWords.length

    //save the parameters
    sc.parallelize(List(docSize + " " + vSize + " " + corpusSize + " " + avgDocSize), numSlices = 1).saveAsTextFile(option.dataCoeff)

    //create dictionary
    val wordIndexMap = new HashMap[String, Int]()
    for (i <- 0 until vSize) {
      wordIndexMap(allWords(i)) = i
    }

    //save the index word dictionary
    saveWordIndexMap(sc, wordIndexMap, option)

    //read sentiment dictionary
    val sentDict = sc.textFile(option.sentDict).map(l => {
      val p = l.split("\t")
      var tmp_sent_pos = 1
      var sent_val = 0.0
      for(i <- 1 to 3){
        if(p(i).toDouble > sent_val){
          sent_val = p(i).toDouble
          tmp_sent_pos = i - 1
        }
      }
      (p(0).trim, tmp_sent_pos)
    })

    //calculate lamda
    val index = new ArrayBuffer[(String, Int)](wordIndexMap.size)
    for(item <- wordIndexMap){
      index.+=((item._1, item._2))
    }
    sc.parallelize(index).leftOuterJoin(sentDict).map(l => {
      (l._2._1, l._2._2.getOrElse(-1))//(index, sentPos)
    }).map(l => l._1 + "\t" + l._2).repartition(1).saveAsTextFile(option.indexSentPos)

    //transform the format of training corpus
    trainFile.filter(l => l.split("\t").length == 4).flatMap(l => {
      val p = l.split("\t")
      val word = p(3).split(" ")
      for(i <- 1 until word.length) yield (word(i), (p(1), p(2)))//(word, (weiboid, userid))
    }).leftOuterJoin(sentDict).map(l => (l._2._1, (wordIndexMap(l._1).toString, l._2._2.getOrElse("-1")))).groupByKey().map(l => {
      val list = l._2
      var result = l._1._2 + "\t" + l._1._1
      for(item <- list) {
        result += "\t" + item._1 + " " + item._2
      }
      result
    }).saveAsTextFile(option.numerTrainFile)//result uid  wid  wordindex1 senti1  wordindex2 senti2

  }

  def main(args : Array[String]): Unit ={
    val option = new SparkJstOption
    jstFileProcess(option)
  }
}
