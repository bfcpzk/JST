package sparkjst.process;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhaokangpan on 2016/12/6.
 */
public class RemoveDuplication {

    public static void main(String[] args) throws IOException{
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("data/senti.txt")),"utf-8"));
        RemoveDuplication rd = new RemoveDuplication();
        rd.removeDup(br);
    }

    public void removeDup(BufferedReader br) throws IOException{
        List<Senti> slist = new ArrayList<Senti>();
        String line = "";
        while((line = br.readLine()) != null){
            String[] p = line.split("\t");
            Senti s = new Senti(p[0], p[1], p[2], p[3]);
            slist.add(s);
        }
        br.close();
        for(int i = 0 ; i < slist.size() ; i++){
            Senti s = slist.get(i);
            for(int j = i + 1 ; j < slist.size() - 1 ; j++){
                if(slist.get(j).word.equals(s.word)){
                    slist.remove(slist.get(j));
                    j--;
                }
            }
        }
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("data/sentiNonDup.txt")),"utf-8"));
        for(int i = 0 ; i < slist.size() ; i++){
            Senti s = slist.get(i);
            bw.write(s.word.trim() + "\t" + s.pos + "\t" + s.neg + "\t" + s.neu + "\n");
        }
        bw.close();
    }

    class Senti{
        String word;
        String pos;
        String neg;
        String neu;

        Senti(String word, String pos, String neg, String neu){
            this.word = word;
            this.pos = pos;
            this.neg = neg;
            this.neu = neu;
        }
    }
}
