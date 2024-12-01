package org.example.Stream;

public class Tuple<K, V> {
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
