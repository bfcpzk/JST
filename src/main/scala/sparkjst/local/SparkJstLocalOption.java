package sparkjst.local;

import java.io.Serializable;

/**
 * Created by zhaokangpan on 2016/11/21.
 */
public class SparkJstLocalOption implements Serializable{

    public String trainFile = "data/testWeibo.txt";
    public String numerTrainFile = "data/numerTrainFile";
    public String sentDict = "data/sentiNonDup.txt";
    public String wordMap = "data/wordMap";
    public String numerSentDict = "data/numerSentDict";
    public String dataCoeff = "data/dataCoeff";
    public int kTopic = 10;
    public double alpha = -1.0;
    public double beta = 0.01;
    public double gamma = -1.0;
    public int maxIter = 100;
    public boolean remote = false;
    public int nSentLabs = 3;
    public int iterFlag = 110;
    public String piOutput = "data/pi_dl";
    public String thetaOutput = "data/theta_dlz";
    public String phiOutput = "data/phi_lzw";

    public String indexSentPos = "data/indexSentPos";

    public int numDocs = 520;
    public int vocabSize = 3134;
    public int corpusSize = 8450;
    public double aveDocLength = 16.25;

}
