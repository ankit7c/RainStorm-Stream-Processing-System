package org.example.Executor;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
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
        if(operationName.equals("op1")) {
            classDir = new File("C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4");
            fullClassName = "Split";
            methodName = "split";
        }else if(operationName.equals("op2")) {
            classDir = new File("C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4");
            fullClassName = "WordCount";
            methodName = "processWords";
            saveMethodName = "saveState";
            loadMethodName = "loadState";
            savePath = "C:\\Users\\saura\\Documents\\Distributed_Systems\\MP4\\";
            saveFileName = "word_count.ser";
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

    public static Object executeCode(List<String> inputList) throws Exception {

        Method method = wordCount.getMethod(methodName, List.class);
        return method.invoke(instance, inputList);
    }
}
