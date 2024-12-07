import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Count implements Serializable {

    public class Tuple<K, V>  implements Serializable {
        private static final long serialVersionUID = 1L;
        private String id;
        private K key;
        private V value;

        public Tuple(String id, K key, V value) {
            this.id = id;
            this.key = key;
            this.value = value;
        }

        public String getId() {
            return id;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }


    }


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

    public List<Tuple> countCategory(Tuple obj,String pattern) throws IOException {
        List<Tuple> resultList = new ArrayList<>();
        try {
            Tuple<String, String> tuple = null;
            if (obj instanceof Tuple<?, ?>) {
                tuple = (Tuple) obj;
                String word = tuple.getKey();
                cumulativeCount.put(word, cumulativeCount.getOrDefault(word,0) + 1);
                Tuple outputTuple = new Tuple(tuple.getId()+"_2", word, cumulativeCount.get(word));
                resultList.add(outputTuple);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return resultList;
    }

}