package sparkjst.cluster;

import java.io.Serializable;

/**
 * Created by zhaokangpan on 2016/11/21.
 */
public class SparkJstHongKongOption implements Serializable{

    public String scMaster = "spark://fintech.is.cityu.edu.hk:7077";

    public String trainFile = "hdfs://127.0.0.1:9000/lda/weibo/newDivide.txt";
    public String sentDict = "hdfs://127.0.0.1:9000/lda/weibo/sentiNonDup.txt";
    public String numerTrainFile = "hdfs://127.0.0.1:9000/lda/weibo/pre/numerTrainFile";
    public String wordMap = "hdfs://127.0.0.1:9000/lda/weibo/pre/wordMap";
    public String dataCoeff = "hdfs://127.0.0.1:9000/lda/weibo/pre/dataCoeff";
    public String indexSentPos = "hdfs://127.0.0.1:9000/lda/weibo/pre/indexSentPos";


    public int kTopic = 10;
    public double alpha = -1.0;
    public double beta = 0.01;
    public double gamma = -1.0;
    public int maxIter = 5;
    public boolean remote = true;
    public int nSentLabs = 3;
    public int iterFlag = 5;
    public double phiCoeff = 0.0001;

    public String tmp_piOutput = "hdfs://127.0.0.1:9000/lda/weibo/result/pi_dl";
    public String tmp_thetaOutput = "hdfs://127.0.0.1:9000/lda/weibo/result/theta_dlz";
    public String tmp_phiOutput = "hdfs://127.0.0.1:9000/lda/weibo/result/phi_lzw";

    public String pathDocument1="hdfs://127.0.0.1:9000/lda/weibo/temp/gibbsLDAtmp_final_";
    public String pathDocument2="hdfs://127.0.0.1:9000/lda/weibo/temp/gibbsLDAtmp2_final_";


    public String piOutput = "hdfs://127.0.0.1:9000/lda/weibo/result/pi_dl";
    public String thetaOutput = "hdfs://127.0.0.1:9000/lda/weibo/result/theta_dlz";
    public String phiOutput = "hdfs://127.0.0.1:9000/lda/weibo/result/phi_lzw";


    public int numDocs = 1024483;
    public int vocabSize = 442067;
    public int corpusSize = 20471070;
    public double aveDocLength = 19.98185426210098;

}
