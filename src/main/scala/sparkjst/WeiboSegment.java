package sparkjst;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.CustomDictionary;
import com.hankcs.hanlp.seg.common.Term;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Created by zhaokangpan on 2016/12/5.
 */
public class WeiboSegment {


    final static List<String> showAllFiles(File dir) throws IOException {
        File[] fs = dir.listFiles();
        List<String> fileNameList = new ArrayList<String>();
        for (int i = 0; i < fs.length; i++) {
            fileNameList.add(fs[i].getCanonicalPath());
            if (fs[i].isDirectory()) {
                try {
                    showAllFiles(fs[i]);
                } catch (Exception e) {
                }
            }
        }
        return fileNameList;
    }

    public static String StringFilter(String str) throws PatternSyntaxException {
        // 只允许字母和数字
        // String regEx = "[^a-zA-Z0-9]";
        // 清除掉所有特殊字符
        String regEx="[`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？《》]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        return m.replaceAll("").trim();
    }


    public static void main(String[] args) throws IOException{

        BufferedReader br_senti = new BufferedReader(new InputStreamReader(new FileInputStream(new File("data/senti.txt")),"utf-8"));
        String line = "";

        //avoid dividing wrongly
        while((line = br_senti.readLine()) != null) {
            String[] p = line.split("\t");
            CustomDictionary.add(p[0].trim(), "nz 1024 n 1");
        }

        //process
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File("Divide.txt")),"utf-8"));

        List<String> fileNameList = showAllFiles(new File("weibodata/"));
        for(int i = 0 ; i < fileNameList.size() ; i++){
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(fileNameList.get(i))),"utf-8"));
            while((line = br.readLine()) != null){
                String[] p = line.split("\t");
                if(p.length == 8){
                    line = p[0] + "\t" + p[1] + "\t" + p[3] + "\t" ;
                    List<Term> ll = HanLP.segment(StringFilter(p[4]));
                    for(int j = 0 ; j < ll.size() ; j ++){
                        if(ll.get(j).toString().contains("/n") || ll.get(j).toString().contains("/a"))
                            line += ll.get(j).toString().split("/")[0] + " ";
                    }
                }else{
                    continue;
                }
                bw.write(line + "\n");
            }
            br.close();
        }
        bw.close();
    }
}
