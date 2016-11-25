package javajst;

import org.kohsuke.args4j.Option;

public class LDACmdOption {
	
	@Option(name="-est", usage="Specify whether we want to estimate model from scratch")
	public boolean est = false;
	
	@Option(name="-inf", usage="Specify whether we want to do inference")
	public boolean inf = true;
	
	@Option(name="-nsentiLabs", usage="Specify the number of sentiment labels")
	public int nsentiLabs = 3;
	
	@Option(name="-ntopics", usage="Specify the number of topics")
	public int ntopics = 10;
	
	@Option(name="-niters", usage="Specify the number of iterations")
	public int niters = 1000;
	
	@Option(name="-savestep", usage="Specify the number of steps to save the model since the last save")
	public int savestep = 100;
	
	@Option(name="-twords", usage="Specify the number of most likely words to be printed for each topic")
	public int twords = 1000;
	
	@Option(name="-data_dir", usage="The directory where input training data is stored")
	public String data_dir = "";
	
	@Option(name="-result_dir", usage="The directory where the output models and parameters will be stored")
	public String result_dir = "";
	
	@Option(name="-model_dir", usage="The directory where the output models and parameters will be stored")
	public String model_dir = "";
	
	@Option(name="-datasetFile", usage="The input training data file")
	public String datasetFile = "";
	
	@Option(name="-sentiLexFile", usage="The sentiment lexicon file")
	public String sentiLexFile = "";
	
	@Option(name="-vocab", usage="The vocabulary file")
	public String vocab = "";
	
	@Option(name="-alpha", usage="The hyperparameter of the per-document sentiment specific topic proportion. The default is avgDocLength*0.05/(numSentiLabs*numTopics).")
	public double alpha = -1.0;
	
	@Option(name="-beta", usage="The hyperparameter of the per-corpus sentiment specific topic-word distribution. The default is 0.01.")
	public double beta = -1.0;
	
	@Option(name="-gamma", usage="The hyperparameter of the per-document sentiment proportion. The default is avgDocLength*0.05/numSentiLabs.")
	public double gamma = -1.0;
		
	@Option(name="-model_dir", usage="The directory of the previously trained model. (for inference only).")
	public String dir = "";
	
	@Option(name="-model", usage="The name of the previously trained model. (for inference only).")
	public String modelName = "";
	
	@Option(name="-wordmap", usage="Specify the wordmap file")
	public String wordMapFileName = "wordmap.txt";

}
