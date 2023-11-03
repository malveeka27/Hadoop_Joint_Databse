import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        String tableName = ((FileSplit) context.getInputSplit()).getPath().getName();

        if (tableName.equals("joinA.txt")) {
       
            outKey.set(fields[0]);
            outValue.set("A," + fields[1]);
            context.write(outKey, outValue);
        } else if (tableName.equals("joinB.txt")) {
            String[] subfields = fields[0].split(" ");
            outKey.set(subfields[1]);
            outValue.set("B," + subfields[0] + "," + fields[1]);
            context.write(outKey, outValue);
        }
    }
}
