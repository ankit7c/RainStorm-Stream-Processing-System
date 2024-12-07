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
    public static File classDir;
    public static  String fullClassName;
    public static  String methodName;
    public static  String saveMethodName;
    public static  String loadMethodName;
    public static  String savePath;
    public static  String saveFileName;
    public static Object instance;
    public static Class<?> wordCount;

    public static void set(String operationName) {
        //TODO read the config file to get the above details
//        if(operationName.equals("op1")) {
//            classDir = new File("C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4\\Rain-Storm\\Executables");
//            fullClassName = "Filter";
//            methodName = "filterOnColumn";
////            classDir = new File("C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4");
////            fullClassName = "Split";
////            methodName = "split";
////            methodName = "split";
//        }else if(operationName.equals("op2")) {
//            classDir = new File("C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4\\Rain-Storm\\Executables");
//            fullClassName = "ExtractColumns";
//            methodName = "extract";
//            saveMethodName = "saveState";
//            loadMethodName = "loadState";
//            savePath = "C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4\\";
//            saveFileName = "word_count.ser";
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
                    if (Boolean.parseBoolean(String.valueOf(ExecutorProperties.getexeProperties().get("methodName" + i)))) {
                        saveMethodName = String.valueOf(ExecutorProperties.getexeProperties().get("methodName" + i));
                        savePath = String.valueOf(ExecutorProperties.getexeProperties().get("savePath" + i));
                        saveFileName = String.valueOf(ExecutorProperties.getexeProperties().get("saveFileName" + i));
                        loadMethodName = String.valueOf(ExecutorProperties.getexeProperties().get("loadMethodName" + i));
                    }
                }
            }
        }catch (Exception e){
            System.out.println("Error while searching for executable");
            e.printStackTrace();
        }
    }

    public static void loadInstance() throws Exception {
        URL[] urls = { classDir.toURI().toURL() };
        URLClassLoader classLoader = URLClassLoader.newInstance(urls);
        // Load and instantiate the class
        wordCount = classLoader.loadClass(fullClassName);

        Method getInstanceMethod = wordCount.getMethod("getInstance");
        instance = getInstanceMethod.invoke(null);
    }

    public static void saveCode() throws Exception{
        Method loadStateMethod = wordCount.getMethod(saveMethodName, String.class);
        loadStateMethod.invoke(instance, savePath+saveFileName);
    }

    public static void loadCode() throws Exception {
        File dir = new File(savePath+saveFileName);
        if (dir.exists()) {
            Method loadStateMethod = wordCount.getMethod(loadMethodName, String.class);
            loadStateMethod.invoke(instance, savePath+saveFileName);
        }
    }

    public static Object executeCode(Tuple tuple, String pattern) throws Exception {

        Method method = wordCount.getMethod(methodName,String.class,String.class,String.class);
        return method.invoke(instance, tuple.getKey(), tuple.getValue() ,pattern);
    }
}
