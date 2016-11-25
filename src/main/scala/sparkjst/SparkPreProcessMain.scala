package sparkjst

/**
  * Created by zhaokangpan on 2016/11/25.
  */
object SparkPreProcessMain {

  def main(args : Array[String]): Unit ={

    val opt = new SparkJstOption

    //preprocess
    val model = new SparkFilePreProcess()
    model.jstFileProcess(opt)

  }
}
