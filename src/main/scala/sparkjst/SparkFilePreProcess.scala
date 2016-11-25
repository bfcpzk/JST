package sparkjst

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * Created by zhaokangpan on 2016/11/21.
  */
class SparkFilePreProcess {

  //start spark
  def startSpark(remote: Boolean) = {
    var scMaster = ""
    if (remote) {
      scMaster = "spark://202.112.113.199:7077" // e.g. 集群
    } else {
      scMaster = "local[4]" // e.g. local[4]
    }
    val conf = new SparkConf().setAppName("SparkFilePreProcess").setMaster(scMaster)
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
    val docSize = sc.textFile(option.trainFile).count.toInt
    val corpusSize = sc.textFile(option.trainFile).flatMap(l => {
      val p = l.split("\t|\r|\n| ")
      for(i <- 1 until p.length) yield ("word", 1)
    }).reduceByKey(_+_).map(l => l._2).collect()(0)

    val allWords = sc.textFile(option.trainFile).flatMap(l => {
      val p = l.split("\t|\r|\n| ")
      for(i <- 1 until p.length) yield p(i)
    }).distinct.collect().toList.sortWith(_ < _)

    //calculate the parameters
    val avgDocSize = (corpusSize * 1.0)/docSize
    val vSize = allWords.length

    //save the parameters
    sc.parallelize(List(docSize + " " + vSize + " " + corpusSize + " " + avgDocSize), numSlices = 1).saveAsTextFile(option.dataCoeff)

    val wordIndexMap = new HashMap[String, Int]()
    for (i <- 0 until vSize) {
      wordIndexMap(allWords(i)) = i
    }

    //save the index word dictionary
    saveWordIndexMap(sc, wordIndexMap, option)

    //read sentiment dictionary
    val sentDict = sc.textFile(option.sentDict).map(l => {
      val p = l.split("\t| ")
      var tmp_sent_pos = 1
      var sent_val = 0.0
      for(i <- 1 to 3){
        if(p(i).toDouble > sent_val){
          sent_val = p(i).toDouble
          tmp_sent_pos = i - 1
        }
      }
      (p(0), tmp_sent_pos)
    })

    //calculate lamda
    val index = new ArrayBuffer[(String, Int)](wordIndexMap.size)
    for(item <- wordIndexMap){
      index.+=((item._1, item._2))
    }
    sc.parallelize(index).leftOuterJoin(sentDict).map(l => {
      (l._2._1, l._2._2.getOrElse(-1))//(index, sentPos)
    }).map(l => l._1 + "\t" + l._2).saveAsTextFile(option.indexSentPos)


    //transform the format of training corpus
    sc.textFile(option.trainFile).flatMap(l => {
      val p = l.split("\t|\n|\r| ")
      for(i <- 1 until p.length) yield (p(i), p(0))
    }).leftOuterJoin(sentDict).map(l => (l._2._1, (wordIndexMap(l._1), l._2._2.getOrElse(-1)))).groupByKey().map(l => {
      val list = l._2
      var result = l._1
      for(item <- list) {
        result += "\t" + item._1 + " " + item._2
      }
      result
    }).saveAsTextFile(option.numerTrainFile)
  }
}
