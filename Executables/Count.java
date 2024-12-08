import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Count implements Serializable {

    private static final Count INSTANCE = new Count();
    private Map<String, Integer> cumulativeCount = new HashMap<>();

    // Save state to file
    public void saveState(String filename) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename))) {
            out.writeObject(cumulativeCount);
        }
    }

    // Load state from file
    public void loadState(String filename) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename))) {
            cumulativeCount = (Map<String, Integer>) in.readObject();
        }
    }

    public static Count getInstance() {
        return INSTANCE;
    }

    public Map<String,String> countCategory(String key, String value, String pattern) throws IOException {
       Map<String,String> resultList = new HashMap<>();
        try {
            String word = key;
            cumulativeCount.put(word, cumulativeCount.getOrDefault(word,0) + 1);
            resultList.put(word, String.valueOf(cumulativeCount.get(word)));
        }catch (Exception e){
            e.printStackTrace();
        }
        return resultList;
    }

}