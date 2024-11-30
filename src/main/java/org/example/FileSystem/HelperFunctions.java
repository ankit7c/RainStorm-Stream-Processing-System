package org.example.FileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class HelperFunctions {

    public static long countLines(String filePath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(Path.of(filePath))) {
            return reader.lines().count();
        }
    }

    public static void listFiller(int concurrentClients, List<String> vmNames,int count) {
        if(concurrentClients ==1){
            for(int i=0;i<1000;i++){
                vmNames.add("Machine1");
            }
        } else if (concurrentClients ==2) {
            for(int i=0;i<count;i++){
                vmNames.add("Machine1");
                vmNames.add("Machine2");
            }
        } else if (concurrentClients ==5) {
            for(int i=0;i<count;i++){
                vmNames.add("Machine1");
                vmNames.add("Machine2");
                vmNames.add("Machine3");
                vmNames.add("Machine4");
                vmNames.add("Machine5");
            }
        } else if (concurrentClients ==10) {
            for(int i=0;i<count;i++){
                vmNames.add("Machine1");
                vmNames.add("Machine2");
                vmNames.add("Machine3");
                vmNames.add("Machine4");
                vmNames.add("Machine5");
                //TODO local testing using Only 5 machine change to 10 later.
                vmNames.add("Machine6");
                vmNames.add("Machine7");
                vmNames.add("Machine8");
                vmNames.add("Machine9");
                vmNames.add("Machine10");
            }
        }
    }
}
