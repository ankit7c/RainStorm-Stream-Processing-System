

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FilterOnColumn {

private static final FilterOnColumn INSTANCE = new FilterOnColumn();
    public static FilterOnColumn getInstance() {
        return INSTANCE;
    }
    public List<Tuple> filterOnColumn(Tuple obj,String pattern) throws IOException {
        List<Tuple>resultList = new ArrayList<>();
        try {
            //String pattern = "Unpunched Telespar";
            Tuple<String, String> tuple = null;
            if (obj instanceof Tuple<?, ?>) {
                tuple = (Tuple<String, String>) obj;
            }
            if(tuple !=null){
                String[] parts = tuple.getValue().split(",");
                String sign_post = parts[6];
                if (sign_post.equals(pattern)) {
                    String category = parts[8];
                    Tuple outputTuple = new Tuple<>(tuple.getId()+"_1", category, 1);
                   resultList.add(outputTuple);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return resultList;
    }

}
