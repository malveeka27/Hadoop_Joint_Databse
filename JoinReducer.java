import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    private List<String> tableA = new ArrayList<String>();
    private List<String> tableB = new ArrayList<String>();
    private Text outKey = new Text();
    private Text outValue = new Text();
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        tableA.clear();
        tableB.clear();
        
        // Separate the values from each table
        for (Text value : values) {
            String valueStr = value.toString();
            String keystr   = key.toString();

            if (valueStr.charAt(0)==('A')) {
                 tableA.add(keystr);
                tableA.add(valueStr);
            } else {
                tableB.add(keystr);
                tableB.add(valueStr);
            }
        }
        
        // Perform the join operation
        for(String a: tableA){
        String[] afields = a.toString().split(",");
        for(String b: tableB){
          String[] bfields = b.toString().split(",");
          outkey.set(key.toString())
          outputValue.set(bfeild[1].toString()+"," + "," + bfeild[2].toString() + "," + afeild[1].toString());
          context.write(outkey, outputValue);
        }
                
            }
        }
    }
}
