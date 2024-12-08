package org.example.Executor;

import org.example.Stream.Tuple;
import org.example.entities.ExecutorProperties;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;

public class OperationExecutor {
    public  File classDir;
    public   String fullClassName;
    public   String methodName;
    public   String saveMethodName;
    public   String loadMethodName;
    public   String savePath;
    public   String saveFileName;
    public  Boolean stateful;
    public  Object instance;
    public  Class<?> wordCount;

    public  void set(String operationName, int selfId, String type) {
        //TODO read the config file to get the above details
//        if(operationName.equals("op1")) {
//            classDir = new File("C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4\\Rain-Storm\\Executables");
//            fullClassName = "FilterOnColumn";
//            methodName = "filterOnColumn";
////            classDir = new File("C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4");
////            fullClassName = "Split";
////            methodName = "split";
////            methodName = "split";
//        }else if(operationName.equals("op2")) {
//            classDir = new File("C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4\\Rain-Storm\\Executables");
//            fullClassName = "Count";
//            methodName = "countCategory";
//            saveMethodName = "saveState";
//            loadMethodName = "loadState";
//            savePath = "C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4\\";
//            saveFileName = "count.ser";
//        }

        //REad the app.properties and load the class
        int totExecutables = Integer.parseInt(String.valueOf(ExecutorProperties.getexeProperties().get("totExecutables")));
        try {
            classDir = new File(String.valueOf(ExecutorProperties.getexeProperties().get("exeDir")));
            for (int i = 1; i <= totExecutables; i++) {
                String className = String.valueOf(ExecutorProperties.getexeProperties().get("class" + i));
                if (className.equals(operationName)) {
                    fullClassName = className;
                    methodName = String.valueOf(ExecutorProperties.getexeProperties().get("methodName" + i));
                    stateful = Boolean.parseBoolean(String.valueOf(ExecutorProperties.getexeProperties().get("stateful" + i)));
                    if (Boolean.parseBoolean(String.valueOf(ExecutorProperties.getexeProperties().get("stateful" + i)))) {
                        saveMethodName = String.valueOf(ExecutorProperties.getexeProperties().get("saveMethodName" + i));
                        savePath = String.valueOf(ExecutorProperties.getexeProperties().get("savePath" + i));
                        saveFileName = String.valueOf(ExecutorProperties.getexeProperties().get("saveFileName" + i));
                        loadMethodName = String.valueOf(ExecutorProperties.getexeProperties().get("loadMethodName" + i));
                    }
                }
            }
            System.out.println("stateful name : " + stateful);
            System.out.println("saveMethond name : " + saveMethodName);
            saveFileName = "local\\" + selfId + "_" + type +"_data.ser";
        }catch (Exception e){
            System.out.println("Error while searching for executable");
            e.printStackTrace();
        }
    }

    public  void loadInstance() throws Exception {
        URL[] urls = { classDir.toURI().toURL() };
        URLClassLoader classLoader = URLClassLoader.newInstance(urls);
        // Load and instantiate the class
        wordCount = classLoader.loadClass(fullClassName);

        Method getInstanceMethod = wordCount.getMethod("getInstance");
        instance = getInstanceMethod.invoke(null);
    }

    public  void saveCode() throws Exception{
        if(stateful){
            Method loadStateMethod = wordCount.getMethod(saveMethodName, String.class);
            loadStateMethod.invoke(instance, savePath+saveFileName);
        }
    }

    public  void loadCode() throws Exception {
        if(stateful) {
            File dir = new File(savePath + saveFileName);
            if (dir.exists()) {
                Method loadStateMethod = wordCount.getMethod(loadMethodName, String.class);
                loadStateMethod.invoke(instance, savePath + saveFileName);
            }
        }
    }

    public  Object executeCode(Tuple tuple, String pattern) throws Exception {

        Method method = wordCount.getMethod(methodName,String.class,String.class,String.class);
        return method.invoke(instance, tuple.getKey(), tuple.getValue() ,pattern);
    }
}
