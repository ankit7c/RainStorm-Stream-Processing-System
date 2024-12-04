

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import java.io.*;

public class WordCount implements Serializable {
    private static final WordCount INSTANCE = new WordCount();
    private Map<String, Long> cumulativeWordCount = new HashMap<>();

    // Save state to file
    public void saveState(String filename) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename))) {
            out.writeObject(cumulativeWordCount);
        }
    }

    // Load state from file
    public void loadState(String filename) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(filename))) {
            cumulativeWordCount = (Map<String, Long>) in.readObject();
        }
    }

    public static WordCount getInstance() {
        return INSTANCE;
    }

    public Map<String, Long> processWords(List<String> words) throws IOException {
        // Count current batch
        Map<String, Long> currentBatchCount = words.stream()
                .map(String::toLowerCase)
                .collect(Collectors.groupingBy(
                        word -> word,
                        Collectors.counting()
                ));

        // Update cumulative count
        currentBatchCount.forEach((word, count) ->
                cumulativeWordCount.merge(word, count, Long::sum)
        );
//        saveState("word_count.ser");
        return new HashMap<>(cumulativeWordCount);
    }
}