package pers.ycf;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Reducer implements Runnable{
    private final Integer reducerNo;
    private final List<File> waitingList = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, List<Integer>> shuffledMap = new ConcurrentSkipListMap<>();;
    private Integer receivedTempNum = 0;
    private OutputStreamWriter writer;
    private volatile Boolean latch = true;

    public Reducer(Integer reducerNo) {
        this.reducerNo = reducerNo;
    }

    @Override
    public void run() {
        System.out.println("Reducer " + reducerNo + " is ready");
        while (latch){
            if (waitingList.isEmpty()) continue;
            File file = waitingList.get(0);
            waitingList.remove(0);
            if (++receivedTempNum == Constant.mapperNum) latch = false;
            shuffle(file);
        }
        openWriterStream();
        shuffledMap.forEach(this::reduce);
        closeWriterStream();
        App.master.collect(getOutputFileLocation());
        System.out.println("Reducer " +reducerNo+ " completes");
    }

    private void openWriterStream()  {
        try {
            File file = new File(getOutputFileLocation());
            file.getParentFile().mkdir();//创建文件夹
            file.createNewFile();//如果文件不存在，就创建该文件
            FileOutputStream fos = new FileOutputStream(file,true);//这里构造方法多了一个参数true,表示在文件末尾追加写入
            writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8);//指定以UTF-8格式写入文件
        }catch (IOException e){
            e.printStackTrace();
        }

    }
    private void closeWriterStream() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void readFile(String fileLocation){
        File file = new File(fileLocation);
        waitingList.add(file);
    }

    private void shuffle(File file) {
        try{
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                parseLine(line);
            }
        }catch (IOException e){
            e.printStackTrace();
        }

    }
    private void parseLine(String line){
        // 定义正则表达式模式
        Pattern pattern = Pattern.compile("<(\\w+),\\s*(\\d+)>");
        // 创建Matcher对象并匹配输入字符串
        Matcher matcher = pattern.matcher(line);
        // 检查是否有匹配的结果
        if (matcher.matches()) {
            String word = matcher.group(1);
            Integer number = Integer.parseInt(matcher.group(2));
            if (shuffledMap.containsKey(word))
                shuffledMap.get(word).add(number);
            else
                shuffledMap.put(word, Collections.synchronizedList(new ArrayList<Integer>(){{add(number);}}));
        } else {
            System.out.println("无法解析输入字符串  " + line);
        }
    }

    private void reduce(String key, List<Integer> valueList){
        int sum = 0;
        for (Integer value : valueList){
            sum += value;
            outputFinal(key, sum);
        }
    }

    private void outputFinal(String key, int sum){
        try {
            writer.write("<" + key + ", " + sum + ">\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getOutputFileLocation(){
        return Constant.rootPath + "/reducer-" + reducerNo + "/output.final";
    }
}
