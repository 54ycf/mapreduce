package pers.ycf;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Master implements Runnable {
    private File sourceFile;
    private final List<File> reducerOutputs = Collections.synchronizedList(new ArrayList<>());
    private volatile Boolean latch = true; // 必须加volatile否则while(latch)会被优化无法退出

    @Override
    public void run() {
        System.out.println("Master is ready");
        while (latch);
        handle();
        System.out.println("Master completes");
        listenReducerOutputs();
        mergeReducerOutputs(Constant.outputTextPath, reducerOutputs);
        System.out.println("merge completes");
    }

    private void listenReducerOutputs() {
        while (reducerOutputs.size() < Constant.reducerNum);
    }
    public void readFile(String fileLocation){
        sourceFile = new File(fileLocation);
    }
    public void execute(){
        latch = false;
    }

    private void handle() {
        try {
            splitText();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private void splitText() throws IOException {
        int textLines = getTextLines(sourceFile);
        int range = (textLines+Constant.mapperNum)/Constant.mapperNum;
        int latch = range;

        List<String> lines = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(sourceFile));
        String line;
        int lineNo = 0, partition = 0;
        while ((line = reader.readLine()) != null) {
            lines.add(line);
            lineNo++;
            if (lineNo==latch){
                writeAndDeliver(lines, partition++);
                lines.clear();
                latch += range;
            }
        }
        writeAndDeliver(lines, partition);
        lines.clear();
        reader.close();
    }

    private int getTextLines(File file) throws IOException{
        FileReader in = new FileReader(file);
        LineNumberReader reader = new LineNumberReader(in);
        reader.skip(Long.MAX_VALUE);
        int lines = reader.getLineNumber();
        reader.close();
        return lines+1;
    }

    private void writeAndDeliver(List<String> lines, Integer partition) throws IOException {
        String location = getOutputFileLocation(partition);
        BufferedWriter writer = new BufferedWriter(new FileWriter(location));
        for (String line : lines) {
            writer.write(line);
            writer.newLine();
        }
        writer.close();
        deliver(location, partition);
    }

    private void deliver(String fileLocation, Integer partition){
        Mapper mapper = App.mappers.get(partition);
        mapper.readFile(fileLocation);
        mapper.execute();
    }


    public synchronized void collect(String fileLocation){
        reducerOutputs.add(new File(fileLocation));
    }

    private void mergeReducerOutputs(String outputFile, List<File> inputFiles) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            BufferedReader[] readers = new BufferedReader[inputFiles.size()];
            String[] currentLines = new String[inputFiles.size()];
            boolean[] filesFinished = new boolean[inputFiles.size()];

            // Initialize readers for input files
            for (int i = 0; i < inputFiles.size(); i++) {
                readers[i] = new BufferedReader(new FileReader(inputFiles.get(i)));
                currentLines[i] = readers[i].readLine();
            }

            boolean allFilesFinished = false;

            while (!allFilesFinished) {
                int minIndex = -1;
                String minValue = null;

                // Find the smallest line among current lines of all files
                for (int i = 0; i < inputFiles.size(); i++) {
                    if (!filesFinished[i] && currentLines[i] != null) {
                        if (minValue == null || currentLines[i].compareTo(minValue) < 0) {
                            minIndex = i;
                            minValue = currentLines[i];
                        }
                    }
                }

                // If all files finished reading, exit the loop
                if (minIndex == -1) {
                    allFilesFinished = true;
                    break;
                }

                // Write the smallest line to the output file
                writer.write(minValue);
                writer.newLine();

                // Read the next line from the file that had the smallest line
                currentLines[minIndex] = readers[minIndex].readLine();

                // Check if the file has reached the end
                if (currentLines[minIndex] == null) {
                    filesFinished[minIndex] = true;
                    readers[minIndex].close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getOutputFileLocation(Integer partition){
        return Constant.rootPath + "/partition-" + partition + ".split";
    }
}
