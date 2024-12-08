

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

public class FilterOnColumn {

private static final FilterOnColumn INSTANCE = new FilterOnColumn();
    public static FilterOnColumn getInstance() {
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

    public Map<String,String> filterOnColumn(String key, String value, String pattern) throws IOException {
        Map<String,String> result = new HashMap<>();
        try {
            String[] parts = parseCSVLine(value);
            String sign_post = parts[6];
            if (sign_post.equals(pattern)) {
                String category = parts[8];
                result.put(category,"1");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

}
