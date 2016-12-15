package sparkjst.process;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhaokangpan on 2016/12/14.
 */
public class CombineFile {
    public static void main(String[] args) throws IOException{
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("weiboLdaTestDivide1.txt")),"utf-8"));
        BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(new File("Divide.txt")),"utf-8"));

        List<String> divide = new ArrayList<String>();

        String line = "";

        while((line = br1.readLine()) != null){
            if(line.split("\t").length == 4){
                divide.add(line);
            }
        }
        br1.close();

        int count = 0;
        while ((line = br.readLine()) != null){
            String[] l = line.split("\t");
            if(l.length == 3 && count < 500000){
                count++;
                String additive = l[1] + "\t" + l[1] + "\t" + l[0] + "\t" + l[2];
                divide.add(additive);
            }
        }
        br.close();

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("newDivide.txt")),"utf-8"));
        for(int i = 0 ; i < divide.size() ; i++){
            bw.write(divide.get(i) + "\n");
        }
        bw.close();

    }
}
