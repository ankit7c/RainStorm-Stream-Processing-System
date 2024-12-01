package org.example.Stream;

import org.example.entities.Member;

import java.io.File;
import java.util.List;
import java.util.Scanner;

//TODO based on role a Worker thread will be created and a specific function will be called
public class Worker extends Thread {

    List<Member> source;
    List<Member> op1;
    List<Member> op2;
    String ranges;
    String type;
    String filename;
    String destFileName;
    public Worker(String type, List<Member> source , List<Member> op1, List<Member> op2, String ranges, String filename, String destFileName) {
        this.source = source;
        this.op1 = op1;
        this.op2 = op2;
        this.ranges = ranges;
        this.type = type;
        this.filename = filename;
        this.destFileName = destFileName;
    }
    //TODO Function : Source
    public void source(){
        //TODO send the data to members present in op1 based on a partition function
        //TODO put the below code in a another jar file
        //TODO Get tuples from the code in object
        //------------------------------
        try {
            String[] range = ranges.split(",");
            int startLine = Integer.parseInt(range[0]);
            int endLine = Integer.parseInt(range[1]);
            try (Scanner scanner = new Scanner(new File(filename))) {
                int currentLine = 0;
                while (scanner.hasNextLine()) {
                    currentLine++;
                    String line = scanner.nextLine();
                    if (currentLine >= startLine && currentLine <= endLine) {

                        System.out.println(line);
                    }
                    if (currentLine > endLine) {
                        break; // Stop reading once we've passed the desired range
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        //TODO Pass the tuple to the queue which will send it to next nodes.
        //------------------------------
    }

    //TODO Function : Split
    public void op1(){
        //TODO based on num tasks create a connection
    }

    //TODO Function : Count
    public void op2(){

    }
    //TODO Function : run function for the thread
    public void run(){
        switch(type) {
            case "source":
                source();
                break;
            case "op1":
                op1();
                break;
            case "op2":
                op2();
                break;
        }
    }
}
