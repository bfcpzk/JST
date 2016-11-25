package javajst;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class Model implements Serializable{
	
	
	// model counts
	//vector<int> nd;
	int[] nd;
	//vector<vector<int> > ndl;
	int[][] ndl;
	//vector<vector<vector<int> > > ndlz;
	int[][][] ndlz;
	//vector<vector<vector<int> > > nlzw;
	int[][][] nlzw;
	//vector<vector<int> > nlz;
	int[][] nlz;
	
	// topic and label assignments
	//vector<vector<double> > p;
	double[][] p;
	//vector<vector<int> > z;
	int[][] z;
	//vector<vector<int> > l;
	int[][] l;
	
	// model parameters
	//vector<vector<double> > pi_dl; // size: (numDocs x L)
	double[][] pi_dl;
	//vector<vector<vector<double> > > theta_dlz; // size: (numDocs x L x T) 
	double[][][] theta_dlz;
	//vector<vector<vector<double> > > phi_lzw; // size: (L x T x V)
	double[][][] phi_lzw;
	
	// hyperparameters 
	//vector<vector<double> > alpha_lz; // \alpha_tlz size: (L x T)
	double[][] alpha_lz;
	//vector<double> alphaSum_l; 
	double[] alphaSum_l;
	//vector<vector<vector<double> > > beta_lzw; // size: (L x T x V)
	double[][][] beta_lzw;
	//vector<vector<double> > betaSum_lz;
	double[][] betaSum_lz;
	//vector<vector<double> > gamma_dl; // size: (numDocs x L)
	double[][] gamma_dl;
	//vector<double> gammaSum_d; 
	double[] gammaSum_d;
	//vector<vector<double> > lambda_lw; // size: (L x V) -- for encoding prior sentiment information 
	double[][] lambda_lw;
	//vector<vector<double> > opt_alpha_lz;  //optimal value, size:(L x T) -- for storing the optimal value of alpha_lz after fix point iteration
	double[][] opt_alpha_lz;
	
	Map<String, Integer> word2id; 
	Map<String, Integer> word2senti;
	Map<Integer, String> id2word;
	Map<String, Integer> sentiLex;
	
	String data_dir;
	String datasetFile;
	String result_dir;
	String sentiLexFile;
	String wordmapfile;
	String tassign_suffix;
	String pi_suffix;
	String theta_suffix;
	String phi_suffix;
	String others_suffix;
	String twords_suffix;

	int numTopics;
	int numSentiLabs; 
	int niters;
	int liter;
	int twords;
	int savestep;
	int updateParaStep;
	double _alpha;
	double _beta;
	double _gamma;
	
	
	int numDocs;
	int vocabSize;
	int corpusSize;
	int aveDocLength;
	
	public static LDADataset dataset;
	
	public Model(){
		setDefaultValues();	
	}
	
	/**
	 * Set default values for variables
	 */
	public void setDefaultValues(){
		wordmapfile = "wordmap.txt";
		tassign_suffix = ".tassign";
		pi_suffix = ".pi";
		theta_suffix = ".theta";
		phi_suffix = ".phi";
		others_suffix = ".others";
		twords_suffix = ".twords";
		
		numTopics = 50;
		numSentiLabs = 3;
		vocabSize = 0;
		numDocs = 0;
		aveDocLength = 0;
		
		niters = 1000;
		liter = 0;
		savestep = 2000;
		twords = 20;
		updateParaStep = 40;
		
		_alpha = -1.0;
		_beta = -1.0;
		_gamma = -1.0;
	}
	
	public boolean init(LDACmdOption option){
		//initial the parameters
		data_dir = option.dir;
		datasetFile = option.datasetFile;
		result_dir = option.result_dir;
		sentiLexFile = option.sentiLexFile;
		wordmapfile = option.wordMapFileName;
		
		numTopics = option.ntopics;
		numSentiLabs = option.nsentiLabs;
		niters = option.niters;
		savestep = option.savestep;
		twords = option.twords;
		
		_alpha = option.alpha;
		_beta = option.beta;
		_gamma = option.gamma;
		
		
		return true;
	}
	
	public void excute_model(){
		//New dataset 
		dataset = new LDADataset(result_dir);
		
		//read sentiment lexicon
		if(sentiLexFile != ""){
			dataset.read_senti_lexicon(sentiLexFile);
		}
		
		
		//read train file
		if(datasetFile != ""){
			dataset.read_dataStream(datasetFile);
		}
		
		//System.out.println("numDocs: " + dataset.numDocs);
		
		word2id = dataset.word2id;
		word2senti = dataset.word2senti;
		id2word = dataset.id2word;
		
		init_model_parameters();
		init_estimate();
		estimate();
		
		
	}
	
	public void init_model_parameters(){
		numDocs = dataset.numDocs;
		vocabSize = dataset.vocabSize;
		corpusSize = dataset.corpusSize;
		aveDocLength = dataset.aveDocLength;
		//System.out.println("numDocs: " + numDocs);
		// model counts
		nd = new int[numDocs];
		for (int m = 0; m < numDocs; m++) {
			nd[m]  = 0;
		}

		ndl = new int[numDocs][numSentiLabs];
		for (int m = 0; m < numDocs; m++) {
			//ndl[m].resize(numSentiLabs);
			for (int l = 0; l < numSentiLabs; l++)
			    ndl[m][l] = 0;
		}

		ndlz = new int[numDocs][numSentiLabs][numTopics];
		for (int m = 0; m < numDocs; m++) {
			//ndlz[m].resize(numSentiLabs);
			for (int l = 0; l < numSentiLabs; l++) {
				//ndlz[m][l].resize(numTopics);
				for (int z = 0; z < numTopics; z++)
					ndlz[m][l][z] = 0; 
			}
		}

		nlzw = new int[numSentiLabs][numTopics][vocabSize];
		for (int l = 0; l < numSentiLabs; l++) {
			//nlzw[l].resize(numTopics);
			for (int z = 0; z < numTopics; z++) {
				//nlzw[l][z].resize(vocabSize);
				for (int r = 0; r < vocabSize; r++)
				    nlzw[l][z][r] = 0;
			}
		}

		nlz = new int[numSentiLabs][numTopics];
		for (int l = 0; l < numSentiLabs; l++) {
			//nlz[l].resize(numTopics);
			for (int z = 0; z < numTopics; z++) {
			    nlz[l][z] = 0;
			}
		}

		// posterior P
		//p.resize(numSentiLabs);
		//for (int l = 0; l < numSentiLabs; l++) {
		//	p[l].resize(numTopics);
		//}
		p = new double[numSentiLabs][numTopics];

		// model parameters
		/*pi_dl.resize(numDocs);
		for (int m = 0; m < numDocs; m++) {
			pi_dl[m].resize(numSentiLabs);
		}*/
		pi_dl = new double[numDocs][numSentiLabs];

		/*theta_dlz.resize(numDocs);
		for (int m = 0; m < numDocs; m++) {
			theta_dlz[m].resize(numSentiLabs);
			for (int l = 0; l < numSentiLabs; l++) {
				theta_dlz[m][l].resize(numTopics);
			}
		}*/
		theta_dlz = new double[numDocs][numSentiLabs][numTopics];

		/*phi_lzw.resize(numSentiLabs);
		for (int l = 0; l < numSentiLabs; l++) {
			phi_lzw[l].resize(numTopics);
			for (int z = 0; z < numTopics; z++) {
				phi_lzw[l][z].resize(vocabSize);
			}
		}*/
		phi_lzw = new double[numSentiLabs][numTopics][vocabSize];

		// init hyperparameters
		/*alpha_lz.resize(numSentiLabs);
		for (int l = 0; l < numSentiLabs; l++) {
			alpha_lz[l].resize(numTopics);
		}*/
		alpha_lz = new double[numSentiLabs][numTopics];

		//alphaSum_l.resize(numSentiLabs);
		alphaSum_l = new double[numSentiLabs];
		
		if (_alpha <= 0) {
			_alpha =  (double)aveDocLength * 0.05 / (double)(numSentiLabs * numTopics);
		}

		for (int l = 0; l < numSentiLabs; l++) {
			alphaSum_l[l] = 0.0;
		    for (int z = 0; z < numTopics; z++) {
			    alpha_lz[l][z] = _alpha;
			    alphaSum_l[l] += alpha_lz[l][z];
		    }
		}

		/*opt_alpha_lz.resize(numSentiLabs);
		for (int l = 0; l < numSentiLabs; l++) {
			opt_alpha_lz[l].resize(numTopics);
		}*/
		opt_alpha_lz = new double[numSentiLabs][numTopics];

		//beta
		if (_beta <= 0) _beta = 0.01;

		//beta_lzw.resize(numSentiLabs);
		//betaSum_lz.resize(numSentiLabs);
		beta_lzw = new double[numSentiLabs][numTopics][];
		betaSum_lz = new double[numSentiLabs][numTopics];
		for (int l = 0; l < numSentiLabs; l++) {
			//beta_lzw[l].resize(numTopics);
			//betaSum_lz[l].resize(numTopics);
			for (int z = 0; z < numTopics; z++) {
				betaSum_lz[l][z] = 0.0;
				//beta_lzw[l][z].resize(vocabSize);
				for (int r = 0; r < vocabSize; r++) {
					beta_lzw[l][z][r] = _beta;
				}
			} 		
		}

		// word prior transformation matrix
		//lambda_lw.resize(numSentiLabs);
		lambda_lw = new double[numSentiLabs][vocabSize];
		for (int l = 0; l < numSentiLabs; l++) {
		    //lambda_lw[l].resize(vocabSize);
			for (int r = 0; r < vocabSize; r++) {
				lambda_lw[l][r] = 1.0; 	
			}
		}

		// incorporate prior information into beta
		prior2beta();
		set_gamma();

	}
	
	public void set_gamma(){
		if (_gamma <= 0 ) {
			_gamma = (double)aveDocLength * 0.05 / (double)numSentiLabs;
		}

		//gamma_dl.resize(numDocs);
		//gammaSum_d.resize(numDocs);

		gamma_dl = new double[numDocs][numSentiLabs];
		gammaSum_d = new double[numDocs];
		
		for (int d = 0; d < numDocs; d++) {
			//gamma_dl[d].resize(numSentiLabs);
			gammaSum_d[d] = 0.0;
			for (int l = 0; l < numSentiLabs; l++) {
				gamma_dl[d][l] = _gamma;
				gammaSum_d[d] += gamma_dl[d][l];
			}
		}
	}
	
	public void prior2beta(){
		
		Iterator<String> it = dataset.sentiLex.keySet().iterator();
		while (it.hasNext()){
			String key = it.next();
			Integer value = dataset.sentiLex.get(key);
			
			if(dataset.word2id.containsKey(key)){
				for(int j = 0; j < numSentiLabs; j++){
					if(value == j)
					{
						lambda_lw[j][dataset.word2id.get(key)] = 1;
					}else{
						lambda_lw[j][dataset.word2id.get(key)] = 0;
					}
					
				}
			}
			
		}
		
		for (int l = 0; l < numSentiLabs; l++) {
			for (int z = 0; z < numTopics; z++) {
				betaSum_lz[l][z] = 0.0;
			    for (int r = 0; r < vocabSize; r++) {
				    beta_lzw[l][z][r] = beta_lzw[l][z][r] * lambda_lw[l][r];  
				    betaSum_lz[l][z] += beta_lzw[l][z][r];
			    }
			}
		}
	}
	
	public void compute_phi_lzw(){
		for (int l = 0; l < numSentiLabs; l++)  {
		    for (int z = 0; z < numTopics; z++) {
				for(int r = 0; r < vocabSize; r++) {
					phi_lzw[l][z][r] = (nlzw[l][z][r] + beta_lzw[l][z][r]) / (nlz[l][z] + betaSum_lz[l][z]);
				}
			}
		}
	}
	
	public void compute_pi_dl(){
		for (int m = 0; m < numDocs; m++) {
		    for (int l = 0; l < numSentiLabs; l++) {
			    pi_dl[m][l] = (ndl[m][l] + gamma_dl[m][l]) / (nd[m] + gammaSum_d[m]);
			}
		}
	}
	
	public void compute_theta_dlz(){
		for (int m = 0; m < numDocs; m++) {
		    for (int l = 0; l < numSentiLabs; l++)  {
				for (int z = 0; z < numTopics; z++) {
				    theta_dlz[m][l][z] = (ndlz[m][l][z] + alpha_lz[l][z]) / (ndl[m][l] + alphaSum_l[l]);    
				}
			}
		}
	}
	
	public void save_model(String model_name){
		save_model_tassign(result_dir + model_name + tassign_suffix);
		
		save_model_twords(result_dir + model_name + twords_suffix); 

		save_model_pi_dl(result_dir + model_name + pi_suffix); 

		save_model_theta_dlz(result_dir + model_name + theta_suffix); 

		save_model_phi_lzw(result_dir + model_name + phi_suffix);

		save_model_others(result_dir + model_name + others_suffix); 

	}
	
	public void save_model_tassign(String filename){
		try{
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			
			for (int m = 0; m < dataset.numDocs; m++){
				writer.write(dataset.docs[m].docID + "\n");
				for (int n = 0; n < dataset.docs[m].length; n++){
					writer.write(dataset.docs[m].words[n] + ":" + l[m][n] + ":" + z[m][n] + " ");					
				}
				writer.write("\n");
			}
				
			writer.close();
		}
		catch (Exception e){
			System.out.println("Error while saving model tassign: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void save_model_twords(String filename){
		try{
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(filename), "UTF-8"));
			
			if (twords > vocabSize){
				twords = vocabSize;
			}
			
			
			for(int l = 0; l < numSentiLabs; l++){
				for(int k = 0; k < numTopics; k++){
					
					List<Pair> words_probs = new ArrayList<Pair>();
					for(int w = 0; w < vocabSize; w++){
						Pair p = new Pair(w, phi_lzw[l][k][w], false);
						words_probs.add(p);
					}
					writer.write("Label " + l + "_Topic " + k + ":\n");
					Collections.sort(words_probs);
					
					
					for(int i = 0; i < twords; i++){
						if(id2word.containsKey((Integer)words_probs.get(i).first)){
							writer.write(id2word.get((Integer)words_probs.get(i).first) + " " + words_probs.get(i).second + "\n");
						}
					}
				}
			}
			
			writer.close();
			
		}
		catch(Exception e){
			System.out.println("Error while saving model twords: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	
	public void save_model_pi_dl(String filename){
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			
			//writer.write("numDocs: " + numDocs + "\n");
			
			for (int m = 0; m < numDocs; m++){
				writer.write("d_" + m + " " + dataset.docs[m].docID + " ");
				for (int l = 0; l < numSentiLabs; l++){
					writer.write(pi_dl[m][l] + " ");
				}
				writer.write("\n");
			}
			writer.close();
		}
		catch (Exception e){
			System.out.println("Error while saving word-topic distribution:" + e.getMessage());
			e.printStackTrace();
		}		
	}
	
	public void save_model_theta_dlz(String filename){
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			
			for (int m = 0; m < numDocs; m++){
				writer.write("document_" + m + "\n");
				for (int l = 0; l < numSentiLabs; l++){
					for(int z = 0; z < numTopics; z++){
						writer.write(theta_dlz[m][l][z] + " ");
					}
					
				}
				writer.write("\n");
			}
			writer.close();
		}
		catch (Exception e){
			System.out.println("Error while saving word-topic distribution:" + e.getMessage());
			e.printStackTrace();
		}		
	}
	
	public void save_model_phi_lzw(String filename){
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			

			for (int l = 0; l < numSentiLabs; l++){
				for(int z = 0; z < numTopics; z++){
					writer.write("Label: " + l + " Topic: " + z + "\n");
					for(int r = 0; r < vocabSize; r++){
						writer.write(phi_lzw[l][z][r] + " ");
					}
					writer.write("\n");
				}				
			}				
			writer.close();
		}
		catch (Exception e){
			System.out.println("Error while saving word-topic distribution:" + e.getMessage());
			e.printStackTrace();
		}		
	}
	
	public void save_model_others(String filename){
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			
			writer.write("data_dir = "+ data_dir + "\n");
			writer.write("datasetFile = " + datasetFile + "\n");
			writer.write("result_dir = " + result_dir + "\n");
			writer.write("sentiLexFile = " + sentiLexFile + "\n");
			
			writer.write("\n---------------------------Corpus Statistics----------------------\n");
			writer.write("numDocs = "+ numDocs + "\n");
			writer.write("corpusSize = " + corpusSize + "\n");
			writer.write("aveDocLength = " + aveDocLength + "\n");
			writer.write("vocabSize = " + vocabSize + "\n");
			
			
			writer.write("\n---------------------------Model Settings----------------------\n");
			writer.write("numSentiLabs = "+ numSentiLabs + "\n");
			writer.write("numTopics = " + numTopics + "\n");
			writer.write("liter = " + liter + "\n");
			writer.write("savestep = " + savestep + "\n");
			
			writer.write("_alpha = "+ _alpha + "\n");
			writer.write("_beta = " + _beta + "\n");
			writer.write("_gamma = " + _gamma + "\n");
			
			
							
			writer.close();
		}
		catch (Exception e){
			System.out.println("Error while saving word-topic distribution:" + e.getMessage());
			e.printStackTrace();
		}	
		
	}
	
	public void init_estimate(){
		int sentiLab, topic;
		//srand(time(0)); // initialize for random number generation
		//z.resize(numDocs);
		//l.resize(numDocs);
		
		z = new int[numDocs][];
		l = new int[numDocs][];

		for (int m = 0; m < numDocs; m++) {
			//int docLength = pdataset->pdocs[m]->length;
			int docLength = dataset.docs[m].length;
			//z[m].resize(docLength);
			//l[m].resize(docLength);
			z[m] = new int[docLength];
			l[m] = new int[docLength];

	        for (int t = 0; t < docLength; t++) {
			    /*if (pdataset->pdocs[m]->words[t] < 0) {
				    printf("ERROE! word token %d has index smaller than 0 at doc[%d][%d]\n", pdataset->pdocs[m]->words[t], m, t);
					return 1;
				}*/
	        	
	        	if((dataset.docs[m].priorSentiLabels[t] > -1) && (dataset.docs[m].priorSentiLabels[t] < numSentiLabs)){
	        		sentiLab = dataset.docs[m].priorSentiLabels[t];
	        	}else{
	        		sentiLab = (int)( Math.random() * numSentiLabs);
				    if (sentiLab == numSentiLabs) sentiLab = numSentiLabs -1;  // to avoid over array boundary
	        	}

	    	   
	    	    l[m][t] = sentiLab;

				// random initialize the topic assginment
				topic = (int)(Math.random() * numTopics);
				if (topic == numTopics)  topic = numTopics - 1; // to avoid over array boundary
				z[m][t] = topic;

				// model count assignments
				nd[m]++;
				ndl[m][sentiLab]++;
				ndlz[m][sentiLab][topic]++;				
				nlzw[sentiLab][topic][dataset.docs[m].words[t]]++;
				nlz[sentiLab][topic]++;
	        }
	    }
	}
	
	public void estimate(){
		int sentiLab = 0, topic = 0;
		//mapname2labs::iterator it;

		//printf("Sampling %d iterations!\n", niters);
		
		System.out.println("Sampling " + niters + " iterations!");
		
		for (liter = 1; liter <= niters; liter++) {
		    //printf("Iteration %d ...\n", liter);
		    System.out.println("Iteration " + liter + " ...");
			for (int m = 0; m < numDocs; m++) {
			    for (int n = 0; n < dataset.docs[m].length; n++) {
					int[] senti2topic = new int[2];
					senti2topic = sampling(m, n);
					l[m][n] = senti2topic[0];
					z[m][n] = senti2topic[1];
				}
			}
			
			//if (updateParaStep > 0 && liter % updateParaStep == 0) {
			//	this->update_Parameters();
			//}
			
			if (savestep > 0 && liter % savestep == 0) {
				if (liter == niters) break;

				//printf("Saving the model at iteration %d ...\n", liter);
				System.out.println("Saving the model at iteration " + liter + " ...");
				compute_pi_dl();
				compute_theta_dlz();
				compute_phi_lzw();
				save_model("model_" + liter);
			}
		}
		
		//printf("Gibbs sampling completed!\n");
		//printf("Saving the final model!\n");
		System.out.println("Gibbs sampling completed!");
		System.out.println("Saving the final model!");
		compute_pi_dl();
		compute_theta_dlz();
		compute_phi_lzw();
		save_model("model_final");
	}
	
	public int[] sampling(int m, int n){
		int sentiLab = l[m][n];
		int topic = z[m][n];
		//int w = pdataset->pdocs[m]->words[n]; 
		// the ID/index of the current word token in vocabulary 
		int w = dataset.docs[m].words[n];
		double u;
		
		nd[m]--;
		ndl[m][sentiLab]--;
		ndlz[m][sentiLab][topic]--;
		nlzw[sentiLab][topic][dataset.docs[m].words[n]]--;
		nlz[sentiLab][topic]--;

		// do multinomial sampling via cumulative method
		for (int l = 0; l < numSentiLabs; l++) {
			for (int k = 0; k < numTopics; k++) {
				p[l][k] = (nlzw[l][k][w] + beta_lzw[l][k][w]) / (nlz[l][k] + betaSum_lz[l][k]) *
			   		(ndlz[m][l][k] + alpha_lz[l][k]) / (ndl[m][l] + alphaSum_l[l]) *
					(ndl[m][l] + gamma_dl[m][l]) / (nd[m] + gammaSum_d[m]);
			}
		}
		
		// accumulate multinomial parameters
		for (int l = 0; l < numSentiLabs; l++) {
			for (int k = 0; k < numTopics; k++) {
				if (k==0) {
				    if (l==0) continue;
			        else p[l][k] += p[l-1][numTopics-1]; // accumulate the sum of the previous array
				}
				else p[l][k] += p[l][k-1];
			}
		}

		// probability normalization
		u = Math.random() * p[numSentiLabs-1][numTopics-1];

		// sample sentiment label l, where l \in [0, S-1]
		boolean loopBreak = false;
		for (sentiLab = 0; sentiLab < numSentiLabs; sentiLab++) {
			for (topic = 0; topic < numTopics; topic++) {
			    if (p[sentiLab][topic] > u) {
			        loopBreak = true;
			        break;
			    }
			}
			if (loopBreak == true) {
				break;
			}
		}
	    
		if (sentiLab == numSentiLabs) sentiLab = numSentiLabs - 1; // to avoid over array boundary
		if (topic == numTopics) topic = numTopics - 1;

		// add estimated 'z' and 'l' to count variables
		nd[m]++;
		ndl[m][sentiLab]++;
		ndlz[m][sentiLab][topic]++;
		nlzw[sentiLab][topic][dataset.docs[m].words[n]]++;
		nlz[sentiLab][topic]++;
		
		int[] senti2topic = new int[2];
		senti2topic[0] = sentiLab;
		senti2topic[1] = topic;
		
		return senti2topic;
	}
}