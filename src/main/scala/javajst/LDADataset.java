package javajst;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

//import Model;

public class LDADataset {
	public Map<String, Integer> word2id; 
	public Map<String, Integer> word2senti;
	public Map<Integer, String> id2word;
	public Map<String, Integer> sentiLex;
	
	public Document [] docs; 		// a list of documents	
	public Document [] _docs;
	ArrayList<String> newWords;
	
	String data_dir;
	String result_dir;
	String wordmapfile;

	int numDocs;
	int aveDocLength; // average document length
	int vocabSize;
	int corpusSize;
	
	
	public LDADataset(){
		word2id = new HashMap<String, Integer>();
		word2senti = new HashMap<String, Integer>();
		id2word = new HashMap<Integer, String>();
		sentiLex = new HashMap<String, Integer>();
		
		docs = null;
		_docs = null;
		
		result_dir = ".";
		wordmapfile = "wordmap.txt";

		numDocs = 0;
		aveDocLength = 0;
		vocabSize = 0;
		corpusSize = 0;
	}
	
	public LDADataset(String result_dir){
		word2id = new HashMap<String, Integer>();
		word2senti = new HashMap<String, Integer>();
		id2word = new HashMap<Integer, String>();
		sentiLex = new HashMap<String, Integer>();
		
		docs = null;
		_docs = null;
		
		this.result_dir = result_dir;
		wordmapfile = "wordmap.txt";

		numDocs = 0;
		aveDocLength = 0;
		vocabSize = 0;
		corpusSize = 0;
	}
	
	public void read_dataStream(String filename){
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(filename), "UTF-8"));
			String line;
			numDocs = 0;
			while((line = reader.readLine()) != null){
				//setDoc(line, numDocs);
				//System.out.println(line);
				numDocs++;
			}
			
			docs = new Document[numDocs];
			
			reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(filename), "UTF-8"));
			int line_num = 0;
			while((line = reader.readLine()) != null){
				setDoc(line, line_num);
				line_num++;
			}
			
			
			vocabSize = word2id.size();
			aveDocLength = corpusSize/numDocs;
			
			
			write_wordmap(result_dir + wordmapfile, word2id);
			read_wordmap(result_dir + wordmapfile);
			
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void setDoc(Document doc, int idx){
		if (0 <= idx && idx < numDocs){
			docs[idx] = doc;
		}
	}
	
	public void setDoc(String str, int idx){
		String [] words = str.split("[ \\t\\n]");
		Vector<Integer> ids = new Vector<Integer>();
		Vector<Integer> labels = new Vector<Integer>();
		
		corpusSize += (words.length - 1);
		
		int flag = 0;
		
		for (String word : words){
			int label = -1;
			int id = -1;
			
			if(flag != 0){
				if(!word2id.containsKey(word)){
					//Sentiment of the current word
					if(sentiLex.containsKey(word)){
						label = sentiLex.get(word);
					}
					id = word2id.size();
					word2id.put(word, id);
					word2senti.put(word, label);
					ids.add(id);
					labels.add(label);
				}else{
					id = word2id.get(word);
					label = word2senti.get(word);
					ids.add(id);
					labels.add(label);
				}	
			}
			flag++;
		}
		
		Document doc = new Document(ids,labels, str);
		docs[idx] = doc;	
		doc.docID = words[0];
	}
	
	
	public void write_wordmap(String filename, Map<String, Integer> word2id){
		try{
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(filename), "UTF-8"));
			
			//write number of words
			writer.write(word2id.size() + "\n");
			
			//write word to id
			Iterator<String> it = word2id.keySet().iterator();
			while (it.hasNext()){
				String key = it.next();
				Integer value = word2id.get(key);
				
				writer.write(key + " " + value + "\n");
			}
			
			writer.close();
		}
		catch (Exception e){
			System.out.println("Error while writing word map " + e.getMessage());
			e.printStackTrace();
		}
		
	}
	
	//public void read_wordmap(String filename, Map<Integer, String> id2word){
	public void read_wordmap(String filename){
		try{
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(filename), "UTF-8"));
			String line;
			
			//read the number of words
			line = reader.readLine();			
			int nwords = Integer.parseInt(line);
			//System.out.println("num: " + nwords);
			
			//read map
			for (int i = 0; i < nwords; ++i){
				line = reader.readLine();
				//System.out.println(line);
				StringTokenizer tknr = new StringTokenizer(line, " ");
				
				if (tknr.countTokens() != 2) continue;
				
				String word = tknr.nextToken();
				String id = tknr.nextToken();
				int intID = Integer.parseInt(id);
				
				id2word.put(intID, word);
				word2id.put(word, intID);
			}
			//this.id2word = id2word;
			reader.close();
		}
		catch (Exception e){
			System.out.println("Error while reading dictionary:" + e.getMessage());
			e.printStackTrace();
		}		
	}
	
	/*public void read_wordmap(String filename, Map<String, Integer> word2id){
		try{
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(filename), "UTF-8"));
			String line;
			
			//read the number of words
			line = reader.readLine();			
			int nwords = Integer.parseInt(line);
			
			//read map
			for (int i = 0; i < nwords; ++i){
				line = reader.readLine();
				StringTokenizer tknr = new StringTokenizer(line, " \t\n\r");
				
				if (tknr.countTokens() != 2) continue;
				
				String word = tknr.nextToken();
				String id = tknr.nextToken();
				int intID = Integer.parseInt(id);
				
				word2id.put(word, intID);
			}
			
			reader.close();
		}
		catch (Exception e){
			System.out.println("Error while reading dictionary:" + e.getMessage());
			e.printStackTrace();
		}		
	}*/
	
	
	
	public void read_senti_lexicon(String filename){
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new FileInputStream(filename), "UTF-8"));
			String line;
			while((line = reader.readLine()) != null){
				StringTokenizer st = new StringTokenizer(line, " \t\r\n");
				double tmp = 0.0;
				int labID = 0;
				double val = 0.0;
				int k = 1;
				String word = st.nextToken();
				
				//System.out.println(st.countTokens());
				
				if(st.countTokens() < 3)
				{
					System.out.println("Sentiment Lexicon Wrong!");
				}else{
					//System.out.println(word);
					while(st.hasMoreElements())
					{
						val = Double.valueOf(st.nextToken());
						//System.out.print(val + " ");
						if(tmp < val)
						{
							tmp = val;
							labID = k - 1;
						}
						k++;
					}
					//System.out.println();
					sentiLex.put(word, labID);
				}				
			}
			
			if(sentiLex.size() <= 0){
				System.out.println("Can not find any sentiment lexicon in file!");
			}
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
