package javajst;

public class main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LDACmdOption option = new LDACmdOption();
		//option.est = true;
		option.alpha = -1;
		option.beta = 0.01;
		option.gamma = -1;
		
		option.model_dir = "data/";
		
		option.data_dir = "data/";
		option.datasetFile = "data/MR.txt";
		option.result_dir = "data/";
		option.sentiLexFile = "data/mpqa.constraint";
		option.wordMapFileName = "wordmap";
		
		option.modelName = "model_final";
		
		option.ntopics = 10;
		option.nsentiLabs = 3;
		option.niters = 100;
		option.savestep = 50;
		
		option.twords = 20;
		
		Model model = new Model();
		model.init(option);
		model.excute_model();
		
		//inference jst = new inference();
		//jst.init(option);

	}

}
