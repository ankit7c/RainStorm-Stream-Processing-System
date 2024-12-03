package org.example.Executor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Test {
    public static void main(String[] args) throws Exception {

        OperationExecutor.set("op1");

        List<String> batch3 = Arrays.asList("Ankit Hello Hello");
        List<String> batch4 = Arrays.asList("Saurabh");
        OperationExecutor.loadInstance();
        // Function to process data
        List<String> resultLine = (List<String>) OperationExecutor.executeCode(batch3);
        for(String s : resultLine){
            System.out.println(s);
        }
        resultLine = (List<String>) OperationExecutor.executeCode(batch4);

        for(String s : resultLine){
            System.out.println(s);
        }

        List<String> batch1 = Arrays.asList("Ankit", "Hello", "Hello");
        List<String> batch2 = Arrays.asList("Ankit");
        OperationExecutor.set("op2");
        // Function to process data
        OperationExecutor.loadInstance();
        OperationExecutor.loadCode();
        Map<String, Long> result = (Map<String, Long>) OperationExecutor.executeCode(batch1);

        result.forEach((word, count) ->
                System.out.println(word + ": " + count)
        );

        result = (Map<String, Long>) OperationExecutor.executeCode(batch2);

        result.forEach((word, count) ->
                System.out.println(word + ": " + count)
        );

        OperationExecutor.saveCode();

    }
}
