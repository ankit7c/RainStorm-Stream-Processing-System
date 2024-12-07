

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

    public Map<String,String> extract(String key, String value, String pattern){
        Map<String,String> result = new HashMap<>();
        try{
                String[] parts = value.split(",");
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
