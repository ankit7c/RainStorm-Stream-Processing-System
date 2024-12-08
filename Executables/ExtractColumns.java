

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExtractColumns {
    private static final ExtractColumns INSTANCE = new ExtractColumns();
    public static ExtractColumns getInstance() {
        return INSTANCE;
    }

    public String[] parseCSVLine(String line) {
        List<String> values = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder currentValue = new StringBuilder();

        for (int i = 0; i < line.length(); i++) {
            char currentChar = line.charAt(i);

            if (currentChar == '"') {
                // Toggle quote state
                inQuotes = !inQuotes;
            } else if (currentChar == ',' && !inQuotes) {
                // End of a value when not in quotes
                values.add(currentValue.toString().trim());
                currentValue = new StringBuilder();
            } else {
                currentValue.append(currentChar);
            }
        }

        // Add the last value
        values.add(currentValue.toString().trim());

        return values.toArray(new String[0]);
    }

    public Map<String,String> extract(String key, String value, String pattern){
        Map<String,String> result = new HashMap<>();
        try{
                // String[] parts = value.split(",");
                String[] parts = parseCSVLine(value);
                if (parts.length > 3) {
                    String objectId = parts[2].trim();
                    String signType = parts[3].trim();
//                    outputTuple = new Tuple<>(tuple.getId()+"_2", tuple.getKey(), objectId + "," + signType);
                    result.put(key, objectId + "," + signType);
                }
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }
}
