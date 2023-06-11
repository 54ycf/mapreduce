package pers.ycf;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Mapper implements Runnable {
    private final Integer mapperNo;
    private File sourceFile;
    private volatile Boolean latch = true;
    private final OutputStreamWriter[] writers = new OutputStreamWriter[Constant.reducerNum];
    public Mapper(Integer mapperNo) {
        this.mapperNo = mapperNo;
    }

    @Override
    public void run() {
        System.out.println("Mapper " + mapperNo + " is ready");
        while (latch);
        try {
            handle();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Mapper " +mapperNo+ " completes");
    }

    public void readFile(String fileLocation){
        sourceFile = new File(fileLocation);
    }
    public void execute(){
        latch = false;
    }

    public void handle() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(sourceFile));
        openWriterStream();
        String line;
        int index = 0;
        while ((line = reader.readLine()) != null)
            map(index++, line);
        closeWriterStream();

        List<Reducer> reducers = App.reducers;
        for (int i = 0; i < reducers.size(); i++)
            reducers.get(i).readFile(getOutputFileLocation(i));
    }

    private void openWriterStream() throws IOException{
        for (int i = 0; i < Constant.reducerNum; i++) {
            File file = new File(getOutputFileLocation(i));
            FileOutputStream fos;
            file.getParentFile().mkdir();//创建文件夹
            file.createNewFile();//创建该文件
            fos = new FileOutputStream(file, true);//文件末尾追加写入
            writers[i] = new OutputStreamWriter(fos, StandardCharsets.UTF_8);//指定以UTF-8格式写入文件
        }
    }

    private void closeWriterStream() throws IOException{
        for (int i = 0; i < Constant.reducerNum; i++)
            writers[i].close();
    }

    private void map(Integer key, String value) throws IOException {
        // for each word in value
        // OutputTemp(word, 1)
        String[] words = value.split("\\P{Alpha}+"); //"[\\p{Punct}\\s]+"
        for(String word : words){
            word = word.toLowerCase();
            if (word.equals(""))
                continue; //忽略空字符串
            outputTemp(word, 1);
        }
    }

    private void outputTemp(String word, Integer count) throws IOException {
        int hash = Math.abs(word.hashCode());
        int partitionNo = hash % Constant.reducerNum;
        writers[partitionNo].write("<" + word + ", " + count + ">\n");
    }

    private String getOutputFileLocation(Integer partitionNo){
        return Constant.rootPath + "/mapper-" + mapperNo + "/partition-" + partitionNo + ".tmp";
    }

}
