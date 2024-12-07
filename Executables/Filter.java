import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Filter {
    private static final Filter INSTANCE = new Filter();
    public static Filter getInstance() {
        return INSTANCE;
    }

    public Map<String,String> filterOnColumn(String key, String value, String pattern) throws IOException {
        Map<String,String> resultList = new HashMap<>();
        try {
            if(value.contains(pattern)){
                resultList.put(key,value);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return resultList;
    }
}
