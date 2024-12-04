// package org.example.Split

import java.io.IOException;
import java.util.*;

public class Split {
    private static final Split INSTANCE = new Split();

    public List<String> split(List<String> lines) throws IOException {
        List<String> words = new ArrayList<>();
        for (String line : lines) {
            System.out.println(line);
            String[] parts = line.split(" ");
            Collections.addAll(words, parts);
        }
        return new ArrayList<>(words);
    }

    public static Split getInstance() {
        return INSTANCE;
    }
}
