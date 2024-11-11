package org.example.FileSystem;

public class HashFunction {

//    public static int hash(String input) {
//        int asciiSum = 0;
//        for (int i = 0; i < input.length(); i++) {
//            asciiSum += input.charAt(i);  // Cast char to int to get ASCII value
//        }
//        return asciiSum % 16;
//    }

    public static int hash(String input) {
        int hash = 0;
        int prime = 31; // A prime number for multiplier
        int modulus = 13; // We want the hash to be in range 0-15

        // Loop through each character in the string
        for (int i = 0; i < input.length(); i++) {
            hash = (hash * prime + input.charAt(i)) % modulus;
        }

        return hash;
    }


    public static int hashCo(String input) {
        int asciiSum = 0;
        for (int i = 0; i < input.length(); i++) {
            asciiSum += input.charAt(i);  // Cast char to int to get ASCII value
        }
        return (asciiSum % 12 + 8) % 12;
    }

    public static void main(String[] args) {
        System.out.println(hash("Hello World"));
        System.out.println(hash("business_1.txt"));
        System.out.println(hash("business_2.txt"));
        System.out.println(hash("business_3.txt"));
        System.out.println(hash("business_4.txt"));
        System.out.println(hash("business_5.txt"));
        System.out.println(hash("business_6.txt"));
        System.out.println(hash("business_7.txt"));
        System.out.println(hash("business_8.txt"));
        System.out.println(hash("business_9.txt"));
        System.out.println(hash("business_10.txt"));
        System.out.println(hash("business_11.txt"));
        System.out.println(hash("business_12.txt"));
        System.out.println(hash("business_13.txt"));
        System.out.println(hash("business_14.txt"));
        System.out.println(hash("business_15.txt"));
        System.out.println(hash("business_16.txt"));
        System.out.println(hash("business_17.txt"));
        System.out.println(hash("business_18.txt"));
        System.out.println(hash("business_19.txt"));
        System.out.println(hash("business_20.txt"));

        System.out.println(("Machine ----------"));
        System.out.println(hash("Machine1"));
        System.out.println(hash("Machine2"));
        System.out.println(hash("Machine3"));
        System.out.println(hash("Machine4"));
        System.out.println(hash("Machine5"));
        System.out.println(hash("Machine6"));
        System.out.println(hash("Machine7"));
        System.out.println(hash("Machine8"));
        System.out.println(hash("Machine9"));
        System.out.println(hash("Machine10"));
    }
}
