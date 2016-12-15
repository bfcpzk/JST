package sparkjst.cluster;

import java.io.Serializable;

/**
 * Created by zhaokangpan on 2016/11/21.
 */
public class SparkJstOption implements Serializable{

    public String trainFile = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/newDivide.txt";
    public String sentDict = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/sentiNonDup.txt";
    public String numerTrainFile = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/pre/numerTrainFile";
    public String wordMap = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/pre/wordMap";
    public String dataCoeff = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/pre/dataCoeff";
    public String indexSentPos = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/pre/indexSentPos";


    public int kTopic = 10;
    public double alpha = -1.0;
    public double beta = 0.01;
    public double gamma = -1.0;
    public int maxIter = 5;
    public boolean remote = true;
    public int nSentLabs = 3;
    public int iterFlag = 5;
    public double phiCoeff = 0.0001;

    public String tmp_piOutput = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/result/pi_dl";
    public String tmp_thetaOutput = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/result/theta_dlz";
    public String tmp_phiOutput = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/result/phi_lzw";


    public String piOutput = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/result/pi_dl";
    public String thetaOutput = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/result/theta_dlz";
    public String phiOutput = "hdfs://202.112.113.199:9000/user/hduser/zhaokangpan/weibo/result/phi_lzw";


    public int numDocs = 1024483;
    public int vocabSize = 442067;
    public int corpusSize = 20471070;
    public double aveDocLength = 19.98185426210098;

}
