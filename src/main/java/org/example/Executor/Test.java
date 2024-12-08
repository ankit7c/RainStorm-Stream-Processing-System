package org.example.Executor;

import org.example.Stream.Tuple;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class Test {

    public static void testExe() throws Exception {

        String inputFilePath = "C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4\\JarExecutor_Updated\\JarExecutor\\src\\main\\java\\org\\example\\exe\\TrafficSigns_100.csv"; // Path to your CSV file
        int count=0;

        try (Scanner scanner = new Scanner(new File(inputFilePath))) {
            int lineNumber = 0;
            while (scanner.hasNextLine()) {
                lineNumber++;
                String line = scanner.nextLine();
                String tupleKey = inputFilePath + "_" + lineNumber;
                Tuple<String, String> tuple = new Tuple<>(tupleKey, tupleKey, line);

//                OperationExecutor.set("Filter");
                OperationExecutor.loadInstance();

                Map<String,String> result = (Map<String,String>)
                        OperationExecutor.executeCode(
                                tuple,
                                "Streetname");
                List<Tuple> result3 = new ArrayList<>();
                result.forEach((k,v)->{
                    result3.add(new Tuple("id",k,v));
                });

//                OperationExecutor.set("ExtractColumns");
                OperationExecutor.loadInstance();
                for(Tuple currTuple: result3){
                    Map<String,String> result4 = (Map<String,String>)
                            OperationExecutor.executeCode(
                                    currTuple,
                                    "Streetname"
                            );
                    count++;
                    result4.forEach((k,v)->{
                        System.out.println(k + "\t" + v);
                    });
                }


            }
            System.out.println(count);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws Exception {

//        OperationExecutor.set("op1");
//
//        List<String> batch3 = Arrays.asList("Ankit Hello Hello");
//        List<String> batch4 = Arrays.asList("Saurabh");
//        OperationExecutor.loadInstance();
//        // Function to process data
//        List<String> resultLine = (List<String>) OperationExecutor.executeCode(batch3);
//        for(String s : resultLine){
//            System.out.println(s);
//        }
//        resultLine = (List<String>) OperationExecutor.executeCode(batch4);
//
//        for(String s : resultLine){
//            System.out.println(s);
//        }
//
//        List<String> batch1 = Arrays.asList("Ankit", "Hello", "Hello");
//        List<String> batch2 = Arrays.asList("Ankit");
//        OperationExecutor.set("op2");
//        // Function to process data
//        OperationExecutor.loadInstance();
//        OperationExecutor.loadCode();
//        Map<String, Long> result = (Map<String, Long>) OperationExecutor.executeCode(batch1);
//
//        result.forEach((word, count) ->
//                System.out.println(word + ": " + count)
//        );
//
//        result = (Map<String, Long>) OperationExecutor.executeCode(batch2);
//
//        result.forEach((word, count) ->
//                System.out.println(word + ": " + count)
//        );
//
//        OperationExecutor.saveCode();

        String inputFilePath = "C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4\\JarExecutor_Updated\\JarExecutor\\src\\main\\java\\org\\example\\exe\\TrafficSigns_100.csv"; // Path to your CSV file
        int count=0;

        try (Scanner scanner = new Scanner(new File(inputFilePath))) {
            int lineNumber = 0;
            while (scanner.hasNextLine()) {
                lineNumber++;
                String line = scanner.nextLine();
                String tupleKey = inputFilePath + "_" + lineNumber;
                Tuple<String, String> tuple = new Tuple<>(tupleKey, tupleKey, line);
//                OperationExecutor.set("op1");
                OperationExecutor.loadInstance();
                //Code to execute Test 2

                Map<String,String> result = (Map<String,String>)
                        OperationExecutor.executeCode(
                                tuple,
                                "Unpunched Telespar"
                        );
                List<Tuple> result2 = new ArrayList<>();
                result.forEach((k,v)->{
                    result2.add(new Tuple("id",k,v));
                    System.out.println(k + "\t" + v);
                });
//                OperationExecutor.set("op2");
                OperationExecutor.loadInstance();
                OperationExecutor.loadCode();
                for(Tuple currTuple: result2){
                    Map<String,String> result3 = (Map<String,String>)
                            OperationExecutor.executeCode(
                                    currTuple,
                                    "Unpunched Telespar"
                            );
                    result3.forEach((k,v)->{
                    System.out.println(k + "\t" + v);
                    });
                }
                OperationExecutor.saveCode();


//                OperationExecutor.set("op1");
//                OperationExecutor.loadInstance();
//
//                Map<String,String> result = (Map<String,String>)
//                        OperationExecutor.executeCode(
//                                tuple,
//                                "Streetname");
//                List<Tuple> result3 = new ArrayList<>();
//                result.forEach((k,v)->{
//                    result3.add(new Tuple("id",k,v));
////                    System.out.println(k + "\t" + v);
//                });
//
//                OperationExecutor.set("op2");
//                OperationExecutor.loadInstance();
//                for(Tuple currTuple: result3){
//                    Map<String,String> result4 = (Map<String,String>)
//                            OperationExecutor.executeCode(
//                                    currTuple,
//                                    "Streetname"
//                            );
//                    count++;
//                    result4.forEach((k,v)->{
//                        System.out.println(k + "\t" + v);
//                    });
//                }


            }
            System.out.println(count);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
