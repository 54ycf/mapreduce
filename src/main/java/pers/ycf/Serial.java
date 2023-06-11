package pers.ycf;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

public class Serial {
    public void execute() throws IOException {
        File file = new File(Constant.inputTextPath);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        Map<String, Integer> map = new TreeMap<>();
        while ((line= br.readLine()) != null){
            String[] words = line.split("\\P{Alpha}+");
            for (String word : words) {
                if (word.equals(""))continue;
                word = word.toLowerCase();
                if (map.containsKey(word)){
                    map.put(word, map.get(word)+1);
                }else{
                    map.put(word, 0);
                }
            }
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(Constant.outputTextPath));
        map.forEach((k,v)->{
            try {
                writer.write("<" + k + ", " + v +">\n");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        writer.close();
    }
}
