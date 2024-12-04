package org.example.entities;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.example.config.AppConfig.readPropertiesFile;

public class ExecutorProperties {
    public static ConcurrentHashMap<String, Object> exeProperties = new ConcurrentHashMap<>();

    public static void initialize(){
        Properties prop = null;
        try {
            prop = readPropertiesFile("application.properties");
        }catch (IOException e){
            e.printStackTrace();
        }
        if(prop == null){
            throw new RuntimeException("Could not load application properties file");
        }
        int totalExecutors = Integer.parseInt(prop.getProperty("totExecutables"));
        exeProperties.put("totExecutables",Integer.parseInt(prop.getProperty("totExecutables")));
        exeProperties.put("exeDir",String.valueOf(prop.getProperty("exeDir")));
        for(int i = 0; i < totalExecutors; i++){
            exeProperties.put("class"+i,String.valueOf(prop.getProperty("class"+i)));
            exeProperties.put("methodName"+i,String.valueOf(prop.getProperty("methodName"+i)));
            boolean stateful = Boolean.parseBoolean(prop.getProperty("stateful"+i));
            exeProperties.put("stateful"+i,String.valueOf(prop.getProperty("stateful"+i)));
            if(stateful){
                exeProperties.put("saveMethodName"+i,String.valueOf(prop.getProperty("saveMethodName"+i)));
                exeProperties.put("loadMethodName"+i,String.valueOf(prop.getProperty("loadMethodName"+i)));
                exeProperties.put("savePath"+i,String.valueOf(prop.getProperty("savePath"+i)));
                exeProperties.put("saveFileName"+i,String.valueOf(prop.getProperty("saveFileName"+i)));
            }
        }
    }

    public static ConcurrentHashMap<String, Object> getexeProperties(){
        return exeProperties;
    }

    public static void printexeProperties(){
        System.out.println("exeProperties:");
        exeProperties.forEach((key, value) -> System.out.println(key + "=" + value));
    }
}
