package javajst;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class inference {
	
	int numSentiLabs; 
	int numTopics;
	int numDocs;      // for trained model
	int vocabSize;    // for trained model
	int newNumDocs;   // for test set
	int newVocabSize; // for test set

	//vector<vector<vector<int> > > nlzw; // for trained model
	int[][][] nlzw;	
	//vector<vector<int> > nlz;  // for trained model
	int[][] nlz;
    //mapword2atr word2atr;
	//mapword2id word2id; 
	//mapid2word id2word;
	Map<String, Integer> word2id;
	Map<Integer, String> id2word;
	Map<String, Integer> word2senti;
	
	Map<String, Integer> _word2id;
	Map<String, Integer> _word2senti;
	
    //map<int, int> id2_id;
	//map<int, int> _id2id;
	Map<Integer, Integer> id2_id;
	Map<Integer, Integer> _id2id;
	
	//mapword2prior sentiLex; // <string, int> => <word, polarity>
	Map<String, Integer> sentiLex;
	
	//vector<string> newWords;
	//String[] newWords;
	ArrayList<String> newWords;
	
	String model_dir;
	String model_name;
	String data_dir;
	String datasetFile;
	String result_dir;
	String sentiLexFile;
	String wordmapfile;
	String betaFile;

	String tassign_suffix;
	String pi_suffix;
	String theta_suffix;
	String phi_suffix;
	String others_suffix;
	String twords_suffix;

	//dataset * pmodelData;	// pointer to trained model object
    //dataset * pnewData; // pointer to new/test dataset object
	//utils * putils;
	
	
	LDADataset pmodelData;
	LDADataset pnewData;

    int niters;
	int liter;
    int twords;
    int savestep;
	int updateParaStep;

	double _alpha;
	double _beta;
	double _gamma;
	
	//vector<vector<double> > new_p; // for posterior
	double[][] new_p;
	//vector<vector<int> > new_z;
	int[][] new_z;
    //vector<vector<int> > new_l;
	int[][] new_l;
	//vector<vector<int> > z;  // for trained model
	int[][] z;
    //vector<vector<int> > l;  // for trained model
	int[][] l;


	// from NEW/test documents
	//vector<int> new_nd;
	int[] new_nd;
	//vector<vector<int> > new_ndl;
	int[][] new_ndl;
	//vector<vector<vector<int> > > new_ndlz;
	int[][][] new_ndlz;
	//vector<vector<vector<int> > > new_nlzw;
	int[][][] new_nlzw;
	//vector<vector<int> > new_nlz;
	int[][] new_nlz;

	// hyperparameters 
    //vector<vector<double> > alpha_lz; // size: (L x T)
	double[][] alpha_lz;
	//vector<double> alphaSum_l;
	double[] alphaSum_l;
	//vector<vector<vector<double> > > beta_lzw; // size: (L x T x V)
	double[][][] beta_lzw;
	//vector<vector<double> > betaSum_lz;
	double[][] betaSum_lz;
	//vector<double> gamma_l; // size: (L)
	double[] gamma_l;
	//double gammaSum;
	double gammaSum;
	//vector<vector<double> > lambda_lw; // size: (L x V) -- for encoding prior sentiment information 
	double[][] lambda_lw;
	
	// model parameters
	//vector<vector<double> > newpi_dl; // size: (numDocs x L)
	double[][] newpi_dl;
	//vector<vector<vector<double> > > newtheta_dlz; // size: (numDocs x L x T) 
	double[][][] newtheta_dlz;
	//vector<vector<vector<double> > > newphi_lzw; // size: (L x T x V)
	double[][][] newphi_lzw;
	
	
	public inference(){
		numSentiLabs = 0; 
		numTopics = 0;
		numDocs = 0; 
		vocabSize = 0;
		newNumDocs = 0;
		newVocabSize = 0;
		_beta = -1.0;
		
		wordmapfile = "wordmap.txt";
	    tassign_suffix = ".newtassign";
	    pi_suffix = ".newpi";
	    theta_suffix = ".newtheta";
	    phi_suffix = ".newphi";
	    others_suffix = ".newothers";
	    twords_suffix = ".newtwords";
		model_name = "";
		data_dir = "";
		datasetFile = "";
		result_dir = "";
		sentiLexFile = "";

		savestep = 20;
		twords = 20;
		niters = 40;
		
		//putils = new utils();
		//pmodelData = NULL;
		//pnewData = NULL;
		
	}
	
	public void init(LDACmdOption option){
		data_dir = option.dir;
		datasetFile = option.datasetFile;
		result_dir = option.result_dir;
		sentiLexFile = option.sentiLexFile;
		wordmapfile = option.wordMapFileName;
		
		model_name = option.modelName;
		model_dir = option.model_dir;
		
		numTopics = option.ntopics;
		numSentiLabs = option.nsentiLabs;
		niters = option.niters;
		savestep = option.savestep;
		twords = option.twords;
		
		_alpha = option.alpha;
		_beta = option.beta;
		_gamma = option.gamma;
		init_inf(option);
		main_inference();
		
	}
	
	public void read_model_setting(String filename, LDACmdOption option){
		String line;
		numSentiLabs = 0;
		numTopics = 0;
		numDocs = 0;
		vocabSize = 0;

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(filename), "UTF-8"));
			
			System.out.println(filename);
			
			while((line = reader.readLine()) != null){
				String[] values = line.split(" = ");
				
				//for(int ll = 0; ll < values.length; ll++){
				//	System.out.print(values[ll] + " ");
				//}
				//System.out.println();
				
				if(values[0].equals("numSentiLabs")){
					numSentiLabs = Integer.parseInt(values[1]);
				}else{
					if(values[0].equals("numTopics")){
						numTopics = Integer.parseInt(values[1]);
					}else{
						if(values[0].equals("numDocs")){
							numDocs = Integer.parseInt(values[1]);
						}else{
							if(values[0].equals("vocabSize")){
								vocabSize = Integer.parseInt(values[1]);
							}
						}
					}
				}				
			}
			
			//System.out.println(numSentiLabs + " " + numTopics + " " +numDocs + " " + vocabSize + " ");
			
			if (numSentiLabs == 0 || numTopics == 0 || numDocs == 0 || vocabSize == 0) {
				System.out.println("Throw exception in reading model parameter settings!");
			}
			else {
				data_dir = option.data_dir;
				datasetFile = option.datasetFile;
				result_dir = option.result_dir;
				sentiLexFile = option.sentiLexFile;
				model_dir = option.model_dir;
				
				wordmapfile = option.wordMapFileName;
				numTopics = option.ntopics;
				numSentiLabs = option.nsentiLabs;
				niters = option.niters;
				savestep = option.savestep;
				twords = option.twords;
				
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public void load_model(String filename, LDACmdOption option){
		try {
			pmodelData.docs = new Document[numDocs];
			pmodelData.vocabSize = vocabSize;
			pmodelData.numDocs = numDocs;
			
			l = new int[pmodelData.numDocs][];
			z = new int[pmodelData.numDocs][];
			
			String line;
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(filename), "UTF-8"));
			
			
			for(int m = 0; m < numDocs; m++){
				String docID = reader.readLine();
				String secondLine = reader.readLine();
				
				String[] values = secondLine.split(" ");
				int length = values.length;
				
				int[] words = new int[length];
				int[] sentiLabs = new int[length];
				int[] topics = new int[length];
				
				for(int j = 0; j < length; j++){
					String token = values[j];
					String[] details = token.split(":");
					if(details.length != 3){
						System.out.println("Invalid word-sentiment-topic assignment format!");
					}
					
					words[j] = Integer.parseInt(details[0]);
					sentiLabs[j] = Integer.parseInt(details[1]);
					topics[j] = Integer.parseInt(details[2]);
					
				}
				
				Document doc = new Document(length, words);
				pmodelData.setDoc(doc, m);
				
				l[m] = new int[sentiLabs.length];
				for (int j = 0; j < (int)sentiLabs.length; j++) {
					l[m][j] = sentiLabs[j];
				}

				z[m] = new int[topics.length];
				for (int j = 0; j < (int)topics.length; j++) {
					z[m][j] = topics[j];
				}								
			}
			
			//init model counts
			nlzw = new int[numSentiLabs][][];
			for (int l = 0; l < numSentiLabs; l++) {
				nlzw[l] = new int[numTopics][];
				for (int z = 0; z < numTopics; z++) {
					nlzw[l][z] = new int[vocabSize];
					for (int r = 0; r < vocabSize; r++) {
					    nlzw[l][z][r] = 0;
					}
				}
			}

			nlz = new int[numSentiLabs][];
			for (int l = 0; l < numSentiLabs; l++) {
				nlz[l] = new int[numTopics];
				for (int z = 0; z < numTopics; z++) {
		            nlz[l][z] = 0;
				}
			}
			
			// recover count values from trained model
			for (int m = 0; m < pmodelData.numDocs; m++) {
				int docLength = pmodelData.docs[m].length;
				for (int n = 0; n < docLength; n++) {
					int w = pmodelData.docs[m].words[n];
					int sentiLab = l[m][n];
					int topic = z[m][n];
					nlzw[sentiLab][topic][w]++;
					nlz[sentiLab][topic]++;
				}
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void read_newData(String filename){
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(filename), "UTF-8"));
			pmodelData.read_wordmap(model_dir + "wordmap.txt");
			
			
			
			//System.out.println(model_dir + "wordmap.txt");
			
			word2id = new HashMap();
			id2word = new HashMap();
			
			word2id = pmodelData.word2id;
			id2word = pmodelData.id2word;
			word2senti = pmodelData.word2senti;
			
			//read sentiment lexicon file
			pnewData.read_senti_lexicon(sentiLexFile);
			pnewData.newWords = newWords = new ArrayList();
			
			sentiLex = pnewData.sentiLex;
			
			if(word2id.size() <= 0){
				System.out.println("Invalid wordmap!");
			}
			
			//read test data
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(filename), "UTF-8"));
			String line;
			int numDocs = 0;
			while((line = reader.readLine()) != null){
				//setDoc(line, numDocs);
				numDocs++;
			}
			//System.out.println(filename);
			pnewData.numDocs = numDocs;
			pnewData.docs = new Document[numDocs];
			
			pnewData._docs = new Document[numDocs];
			
			pnewData.vocabSize = 0;
			pnewData.corpusSize = 0;
			
			id2_id = new HashMap<Integer, Integer>();
			_id2id = new HashMap<Integer, Integer>();
			
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(filename), "UTF-8"));
			//process each document			
			int line_num = 0;
			while((line = reader.readLine()) != null){
				String[] words = line.split(" ");
				int docLength = words.length;

				System.out.println(line);
				pnewData.corpusSize += (docLength - 1);
				//int[] doc = new int[docLength];
				//int[] _doc = new int[docLength];
				//ArrayList<Integer> priorSentiLabs = new;
				Vector<Integer> ids = new Vector<Integer>();
				Vector<Integer> _ids = new Vector<Integer>();
				Vector<Integer> priorSentiLabs = new Vector<Integer>();
				
				
				//process each token in the document
				for(int k = 1; k < docLength; k++){
					//System.out.println("word[k]: " + words[k]);
					if(!word2id.containsKey(words[k])) // new word
					{
						pnewData.newWords.add(words[k]);
					}else{
						//System.out.println("enter this step");
						int _id;
						if(id2_id.containsKey(word2id.get(words[k]))){
							_id = id2_id.get(word2id.get(words[k]));
						}else{
							_id = id2_id.size();
							id2_id.put(word2id.get(words[k]), _id);
							_id2id.put(_id, word2id.get(words[k]));
							System.out.println(_id + " " + word2id.get(words[k]));
						}
						
						//doc[k-1] = word2id.get(words[k]);
						//_doc[k-1] = _id;
						ids.add(word2id.get(words[k]));
						_ids.add(_id);
						
						//word2senti is specific to new/test dataset
						int priorSenti = -1;
						if(!word2senti.containsKey(words[k])){
							if(sentiLex.containsKey(words[k])){
								priorSenti = sentiLex.get(words[k]);
							}
							// encode sentiment into infor word2senti
							word2senti.put(words[k], priorSenti);
							
						}
						priorSentiLabs.add(priorSenti);
					}
					
				}
				
				Document pdoc = new Document(ids,priorSentiLabs, "inference");
				Document _pdoc = new Document(_ids,priorSentiLabs, "inference");
				
				pdoc.docID = words[0];
				_pdoc.docID = words[0];
				
				// add new doc
				
				pnewData.docs[line_num] = pdoc;
				pnewData._docs[line_num] = _pdoc;
				
				line_num++;
			}
			
			
			
			// update number of new words
			pnewData.vocabSize = id2_id.size();
			pnewData.aveDocLength = pnewData.corpusSize/pnewData.numDocs;
			newNumDocs = pnewData.numDocs;
			newVocabSize = pnewData.vocabSize;
		
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void init_parameters(){
		new_p = new double[numSentiLabs][];
		for (int l = 0; l < numSentiLabs; l++) 	{
			new_p[l] = new double[numTopics];
			for (int z = 0; z < numTopics; z++) {
			    new_p[l][z] = 0.0;
			}
		}

		new_nd = new int[pnewData.numDocs]; 
		for (int m = 0; m < pnewData.numDocs; m++) {
		    new_nd[m] = 0;
		}

		new_ndl = new int[pnewData.numDocs][];
		for (int m = 0; m < pnewData.numDocs; m++) {
			new_ndl[m] = new int[numSentiLabs];
			for (int l = 0; l < numSentiLabs; l++) {
			    new_ndl[m][l] = 0;
			}
		}

		new_ndlz = new int[pnewData.numDocs][][];
		for (int m = 0; m < pnewData.numDocs; m++) {
			new_ndlz[m] = new int[numSentiLabs][];
		    for (int l = 0; l < numSentiLabs; l++)	{
				new_ndlz[m][l] = new int[numTopics];
				for (int z = 0; z < numTopics; z++) {
				    new_ndlz[m][l][z] = 0; 
				}
			}
		}

		new_nlzw = new int[numSentiLabs][][];
		for (int l = 0; l < numSentiLabs; l++) {
			new_nlzw[l] = new int[numTopics][];
			for (int z = 0; z < numTopics; z++) {
				new_nlzw[l][z] = new int[pnewData.vocabSize];
				for (int r = 0; r < pnewData.vocabSize; r++) {
				    new_nlzw[l][z][r] = 0;
				}
			}
		}

		new_nlz = new int[numSentiLabs][];
		for (int l = 0; l < numSentiLabs; l++) {
			new_nlz[l] = new int[numTopics];
			for (int z = 0; z < numTopics; z++) {
			    new_nlz[l][z] = 0;
			}
		}

		// model parameters
		newpi_dl = new double[pnewData.numDocs][];
		for (int m = 0; m < pnewData.numDocs; m++) {
			newpi_dl[m] = new double[numSentiLabs];
		}

		newtheta_dlz = new double[pnewData.numDocs][][];
		for (int m = 0; m < pnewData.numDocs; m++) {
			newtheta_dlz[m] = new double[numSentiLabs][];
			for (int l = 0; l < numSentiLabs; l++) {
				newtheta_dlz[m][l] = new double[numTopics];
			}
		}

		newphi_lzw = new double[numSentiLabs][][];
		for (int l = 0; l < numSentiLabs; l++) {
			newphi_lzw[l] = new double[numTopics][];
			for (int z = 0; z < numTopics; z++) {
				newphi_lzw[l][z] = new double[pnewData.vocabSize];
			}
		}

		// hyperparameters
		_alpha =  (double)pnewData.aveDocLength * 0.05 / (double)(numSentiLabs * numTopics);
		alpha_lz = new double[numSentiLabs][];
		alphaSum_l = new double[numSentiLabs];
		for (int l = 0; l < numSentiLabs; l++) {
			alphaSum_l[l] = 0.0;
			alpha_lz[l] = new double[numTopics];
			for (int z = 0; z < numTopics; z++) {
				alpha_lz[l][z] = _alpha;
				alphaSum_l[l] += alpha_lz[l][z];
			}
		}

		// gamma
		gamma_l = new double[numSentiLabs];
		gammaSum = 0.0;
		for (int l = 0; l < numSentiLabs; l++) {
			gamma_l[l] = (double)pnewData.aveDocLength * 0.05 / (double)numSentiLabs;
			gammaSum += gamma_l[l];
		}

		//beta
		if (_beta <= 0) {
			_beta = 0.01;
		}
		beta_lzw = new double[numSentiLabs][][];
		betaSum_lz = new double[numSentiLabs][];
		for (int l = 0; l < numSentiLabs; l++) {
			beta_lzw[l] = new double[numTopics][];
			betaSum_lz[l] = new double[numTopics];
			for (int z = 0; z < numTopics; z++) {
				beta_lzw[l][z] = new double[pnewData.vocabSize];
				for (int r = 0; r < pnewData.vocabSize; r++) {
					beta_lzw[l][z][r] = _beta; 
					betaSum_lz[l][z] += beta_lzw[l][z][r];
				}
			} 		
		}
		
		// incorporate prior knowledge into beta
		if (sentiLexFile != "") {
			// word prior transformation matrix
			lambda_lw = new double[numSentiLabs][];
			for (int l = 0; l < numSentiLabs; l++) {
			  lambda_lw[l] = new double[pnewData.vocabSize];
				for (int r = 0; r < pnewData.vocabSize; r++)
					lambda_lw[l][r] = 1;
			}
			// MUST init beta_lzw first before incorporating prior information into beta
			prior2beta();
		}
	}
	
	public void prior2beta(){
		
		Iterator<String> it = sentiLex.keySet().iterator();
		while (it.hasNext()){
			String key = it.next();
			Integer value = sentiLex.get(key);
			
			if(word2id.containsKey(key)){
				System.out.println("word:" + key);
				for(int j = 0; j < numSentiLabs; j++){
					if(value == j)
					{
						System.out.println(word2id.get(key));
						System.out.println(id2_id.get(word2id.get(key)));
						lambda_lw[j][id2_id.get(word2id.get(key))] = 1;
					}else{
						lambda_lw[j][id2_id.get(word2id.get(key))] = 0;
					}
					
				}
			}
			
		}

		// Note: the 'r' index of lambda[j][r] is corresponding to the vocabulary ID.
		// Therefore the correct prior info can be incorporated to corresponding word cound nlzw,
		// as 'w' is also corresponding to the vocabulary ID.
		for (int l = 0; l < numSentiLabs; l++) {
			for (int z = 0; z < numTopics; z++) {
				betaSum_lz[l][z] = 0.0;
			    for (int r = 0; r < pnewData.vocabSize; r++) {
				    beta_lzw[l][z][r] = beta_lzw[l][z][r] * lambda_lw[l][r];
				    betaSum_lz[l][z] += beta_lzw[l][z][r];
			    }
			}
		}
	}
	
	public void init_inf(LDACmdOption option){
		pmodelData = new LDADataset();
		pnewData = new LDADataset(result_dir);
		
		read_model_setting(model_dir + model_name + ".others", option);
		
		load_model(model_dir + model_name + ".tassign", option);
		
		read_newData(datasetFile);
		
		init_parameters();
		
		
		int sentiLab, topic;
		
		new_z = new int[pnewData.numDocs][];
		new_l = new int[pnewData.numDocs][];
		
		for (int m = 0; m < pnewData.numDocs; m++) {
			int docLength = pnewData.docs[m].length;
			new_z[m] = new int[docLength];
			new_l[m] = new int[docLength];

			for (int t = 0; t < docLength; t++) {
			    if (pnewData.docs[m].words[t] < 0) {
				    System.out.println("ERROE! word token " + pnewData.docs[m].words[t] 
				    		+ "has index smaller than 0 in doc " + m + "," + t);

				}

				// sample sentiment label
			    if ((pnewData.docs[m].priorSentiLabels[t] > -1) && (pnewData.docs[m].priorSentiLabels[t] < numSentiLabs)) {
				    sentiLab = pnewData.docs[m].priorSentiLabels[t]; // incorporate prior information into the model  
				}
				else {
				    sentiLab = (int)(Math.random() * numSentiLabs);
				    if (sentiLab == numSentiLabs) sentiLab = numSentiLabs -1;
				}
			    new_l[m][t] = sentiLab;

				// sample topic label 
				topic = (int)(Math.random() * numTopics);
				if (topic == numTopics)  topic = numTopics - 1;
				new_z[m][t] = topic;

				new_nd[m]++;
				new_ndl[m][sentiLab]++;
				new_ndlz[m][sentiLab][topic]++;
				new_nlzw[sentiLab][topic][pnewData.docs[m].words[t]]++;
				new_nlz[sentiLab][topic]++;
	       } 
		}
		
		
	}
	
	public void main_inference(){
		int sentiLab, topic;
		System.out.println("Sampling " + niters + " iterations for inference!");

		liter = 0; 
		for (liter = 1; liter <= niters; liter++) {
			System.out.println("Iteration " + liter + "  ...");
			for (int m = 0; m < pnewData.numDocs; m++) {
				for (int n = 0; n < pnewData.docs[m].length; n++) {
					int[] senti2topic = new int[2];
					senti2topic = inf_sampling(m, n);
					new_l[m][n] = senti2topic[0]; 
					new_z[m][n] = senti2topic[1]; 
				} 
			}
			
			if (savestep > 0 && liter % savestep == 0) {
				if (liter == niters) break;
					
				System.out.println("Saving the model at iteration " + liter + " ...");
				compute_newpi();
				compute_newtheta();
				compute_newphi();
				save_model(model_name + "_" + liter);
			}
		}
	    
		System.out.println("Gibbs sampling completed!\n");
		System.out.println("Saving the final model!\n");
		compute_newpi();
		compute_newtheta();
		compute_newphi();
		save_model(model_name);
	}
	
	public void compute_newpi(){
		for (int m = 0; m < pnewData.numDocs; m++) {
		    for (int l = 0; l < numSentiLabs; l++) {
			    newpi_dl[m][l] = (new_ndl[m][l] + gamma_l[l]) / (new_nd[m] + gammaSum);
		    }
		}
	}
	
	public void compute_newtheta(){
		for (int m = 0; m < pnewData.numDocs; m++) {
		    for (int l = 0; l < numSentiLabs; l++)  {
				for (int z = 0; z < numTopics; z++) {
				    newtheta_dlz[m][l][z] = (new_ndlz[m][l][z] + alpha_lz[l][z]) / (new_ndl[m][l] + alphaSum_l[l]);
				}
			}
		}
	}
	
	public void compute_newphi(){
		for (int l = 0; l < numSentiLabs; l++)  {
		    for (int z = 0; z < numTopics; z++) {
				for(int r = 0; r < pnewData.vocabSize; r++) {
				    if(_id2id.containsKey(r)){
				    	newphi_lzw[l][z][r] = (nlzw[l][z][_id2id.get(r)] + new_nlzw[l][z][r] + beta_lzw[l][z][r]) / (nlz[l][z] + new_nlz[l][z] + betaSum_lz[l][z]);
				    }else {
					    System.out.println("Error! Cannot find word " + r + "!"); 
					}
				}
			}
		}
	}
	
	
	
	public int[] inf_sampling(int m, int n){
		int sentiLab = new_l[m][n];
		int topic = new_z[m][n];
		int w = pnewData.docs[m].words[n];   // word index of trained model
		int _w = pnewData._docs[m].words[n]; // word index of test data
		double u;

		
		new_nd[m]--;
		new_ndl[m][sentiLab]--;
		new_ndlz[m][sentiLab][topic]--;
		new_nlzw[sentiLab][topic][_w]--;
		new_nlz[sentiLab][topic]--;

	    // do multinomial sampling via cumulative method
	    for (int l = 0; l < numSentiLabs; l++) {
	  	    for (int k = 0; k < numTopics; k++) {
			    new_p[l][k] = (nlzw[l][k][w] + new_nlzw[l][k][_w] + beta_lzw[l][k][_w]) / (nlz[l][k] + new_nlz[l][k] + betaSum_lz[l][k]) *
				    (new_ndlz[m][l][k] + alpha_lz[l][k]) / (new_ndl[m][l] + alphaSum_l[l]) *
				    (new_ndl[m][l] + gamma_l[l]) / (new_nd[m] + gammaSum);
			}
		}

		// accumulate multinomial parameters
		for (int l = 0; l < numSentiLabs; l++) {
			for (int k = 0; k < numTopics; k++) {
				if (k==0) {
				    if (l==0) continue;
			        else new_p[l][k] += new_p[l-1][numTopics-1];
				}
				else new_p[l][k] += new_p[l][k-1];
		    }
		}
		// probability normalization
		u = Math.random() * new_p[numSentiLabs-1][numTopics-1];
		boolean loopBreak = false;
		for (sentiLab = 0; sentiLab < numSentiLabs; sentiLab++) {
			for (topic = 0; topic < numTopics; topic++) {
			    if (new_p[sentiLab][topic] > u) {
			    	loopBreak = true;
			        break;
			    }
			}
			if (loopBreak == true) {
				break;
			}
		}
	    
		if (sentiLab == numSentiLabs) sentiLab = numSentiLabs - 1; // the max value of label is (S - 1)
		if (topic == numTopics) topic = numTopics - 1; 

		// add estimated 'z' and 'l' to counts
		new_nd[m]++;
		new_ndl[m][sentiLab]++;
		new_ndlz[m][sentiLab][topic]++;
		new_nlzw[sentiLab][topic][_w]++;
		new_nlz[sentiLab][topic]++;
		
		int[] senti2topic = new int[2];
		senti2topic[0] = sentiLab;
		senti2topic[1] = topic;
		
		return senti2topic;
	}
	
	public void save_model(String model_name){
		save_model_newtassign(result_dir + model_name + tassign_suffix);
		
		save_model_newtwords(result_dir + model_name + twords_suffix); 

		save_model_newpi_dl(result_dir + model_name + pi_suffix); 

		save_model_newtheta_dlz(result_dir + model_name + theta_suffix); 

		save_model_newphi_lzw(result_dir + model_name + phi_suffix);

		save_model_newothers(result_dir + model_name + others_suffix); 

	}
	
	public void save_model_newtassign(String filename){
		try{
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			
			for (int m = 0; m < pnewData.numDocs; m++){
				writer.write(pnewData.docs[m].docID + "\n");
				for (int n = 0; n < pnewData.docs[m].length; n++){
					writer.write(pnewData.docs[m].words[n] + ":" + new_l[m][n] + ":" + new_z[m][n] + " ");					
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
	
	public void save_model_newtwords(String filename){
		try{
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(filename), "UTF-8"));
			
			if (twords > vocabSize){
				twords = vocabSize;
			}
			
			
			for(int l = 0; l < numSentiLabs; l++){
				for(int k = 0; k < numTopics; k++){
					
					List<Pair> words_probs = new ArrayList<Pair>();
					for(int w = 0; w < pnewData.vocabSize; w++){
						Pair p = new Pair(w, newphi_lzw[l][k][w], false);
						words_probs.add(p);
					}
					writer.write("Label " + l + "_Topic " + k + ":\n");
					Collections.sort(words_probs);
					
					
					for(int i = 0; i < twords; i++){
						if(_id2id.containsKey((Integer)words_probs.get(i).first)){
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
	
	
	public void save_model_newpi_dl(String filename){
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			
			for (int m = 0; m < numDocs; m++){
				writer.write("d_" + m + " " + pnewData.docs[m].docID + " ");
				for (int l = 0; l < numSentiLabs; l++){
					writer.write(newpi_dl[m][l] + " ");
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
	
	public void save_model_newtheta_dlz(String filename){
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			
			for (int m = 0; m < numDocs; m++){
				writer.write("document_" + m + "\n");
				for (int l = 0; l < numSentiLabs; l++){
					for(int z = 0; z < numTopics; z++){
						writer.write(newtheta_dlz[m][l][z] + " ");
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
	
	public void save_model_newphi_lzw(String filename){
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			

			for (int l = 0; l < numSentiLabs; l++){
				for(int z = 0; z < numTopics; z++){
					writer.write("Label: " + l + " Topic: " + z + "\n");
					for(int r = 0; r < pnewData.vocabSize; r++){
						writer.write(newphi_lzw[l][z][r] + " ");
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
	
	public void save_model_newothers(String filename){
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			
			writer.write("model_dir = "+ model_dir + "\n");
			writer.write("model_name = " + model_name + "\n");
			writer.write("data_dir = "+ data_dir + "\n");
			writer.write("datasetFile = " + datasetFile + "\n");
			writer.write("result_dir = " + result_dir + "\n");
			writer.write("niters = " + niters + "\n");
			writer.write("savestep-inf = " + savestep + "\n");
			
			writer.write("\n---------------------------Test Statistics----------------------\n");
			writer.write("newNumDocs = "+ pnewData.numDocs + "\n");
			writer.write("newCorpusSize = " + pnewData.corpusSize + "\n");
			writer.write("newVocabSize = " + pnewData.vocabSize + "\n");
			writer.write("numNewWords = " + pnewData.newWords.size() + "\n");
			writer.write("aveDocLength = " + pnewData.aveDocLength + "\n");
			
			
			writer.write("\n---------------------------Loaded Model Settings----------------------\n");
			writer.write("numSentiLabs = "+ numSentiLabs + "\n");
			writer.write("numTopics = " + numTopics + "\n");
			writer.write("numDocs = " + pmodelData.numDocs + "\n");
			writer.write("corpusSize = " + pmodelData.corpusSize + "\n");
			writer.write("vocabSize = " + pmodelData.vocabSize + "\n");
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
	

}
