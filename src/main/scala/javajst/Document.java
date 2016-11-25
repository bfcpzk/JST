package javajst;

import java.util.Vector;

public class Document {

	//----------------------------------------------------
	//Instance Variables
	//----------------------------------------------------
	public int [] words;
	public int [] priorSentiLabels;
	public String docID;
	public String rawStr;
	public int length;
	
	//----------------------------------------------------
	//Constructors
	//----------------------------------------------------
	public Document(){
		words = null;
		priorSentiLabels = null;
		docID = "";
		rawStr = "";
		length = 0;
	}
	
	public Document(int length){
		this.length = length;
		docID = "";
		rawStr = "";
		words = new int[length];
		priorSentiLabels = new int[length];
	}
	
	public Document(int length, int [] words){
		this.length = length;
		docID = "";
		rawStr = "";
		
		this.words = new int[length];
		for (int i =0 ; i < length; ++i){
			this.words[i] = words[i];
		}
		priorSentiLabels = new int[length];
	}
	
	public Document(int length, int [] words, String rawStr){
		this.length = length;
		docID = "";
		this.rawStr = rawStr;
		
		this.words = new int[length];
		for (int i =0 ; i < length; ++i){
			this.words[i] = words[i];
		}
		priorSentiLabels = new int[length];
	}
	
	public Document(Vector<Integer> doc){
		this.length = doc.size();
		docID = "";
		rawStr = "";
		this.words = new int[length];
		for (int i = 0; i < length; i++){
			this.words[i] = doc.get(i);
		}
		priorSentiLabels = new int[length];
	}
	
	public Document(Vector<Integer> doc, String rawStr){
		this.length = doc.size();
		docID = "";
		this.rawStr = rawStr;
		this.words = new int[length];
		for (int i = 0; i < length; ++i){
			this.words[i] = doc.get(i);
		}
		priorSentiLabels = new int[length];
	}
	
	public Document(Vector<Integer> doc, Vector<Integer> priorSentiLabels, String rawStr){
		this.length = doc.size();
		docID = "";
		this.rawStr = rawStr;
		this.words = new int[length];
		this.priorSentiLabels = new int[length];
		for (int i = 0; i < length; ++i){
			this.words[i] = doc.get(i);
			this.priorSentiLabels[i] = priorSentiLabels.get(i);
		}
	}
}
