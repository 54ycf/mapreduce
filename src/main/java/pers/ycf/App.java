package pers.ycf;

import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 *
 */
public class App {
    public static Master master;
    public static List<Mapper> mappers = new ArrayList<>();
    public static List<Reducer> reducers = new ArrayList<>();

    public static void main(String[] args ) throws InterruptedException {
        long start = System.currentTimeMillis();
        mapreduce();
        long end = System.currentTimeMillis();
        System.out.println("cost time: " + (end-start) + " milliseconds");
    }

    private static void mapreduce() throws InterruptedException {
        master = new Master();
        Thread masterThread = new Thread(master);
        masterThread.start();
        for (int i = 0; i < Constant.mapperNum; i++){
            Mapper mapper = new Mapper(i);
            new Thread(mapper).start();
            mappers.add(mapper);
        }
        for (int i = 0; i < Constant.reducerNum; i++) {
            Reducer reducer = new Reducer(i);
            new Thread(reducer).start();
            reducers.add(reducer);
        }
        master.readFile(Constant.inputTextPath);
        master.execute();
        masterThread.join();
    }
}
