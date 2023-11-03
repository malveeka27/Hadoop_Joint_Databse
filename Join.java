import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.ArrayList;
import java.util.List;



public class Join
{
public  static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

   
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
public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
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
          
                tableA.add(valueStr);
            } else if (valueStr.charAt(0)==('B')){
               
                tableB.add(valueStr);
            }
        }
        
        // Perform the join operation
        for(String a: tableA){
        String[] afields = a.toString().split(",");
        for(String b: tableB){
          String[] bfields = b.toString().split(",");
          outKey.set(key.toString());
          outValue.set(bfields[1].toString() + "," + bfields[2].toString() + "," + afields[1].toString());
          context.write(outKey, outValue);
        }
        }
        }
        }
public static void main(String[] args) throws Exception 
{
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Join");
job.setJarByClass(Join.class);
job.setMapperClass(JoinMapper.class);
job.setReducerClass(JoinReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileInputFormat.addInputPath(job, new Path(args[1]));
FileOutputFormat.setOutputPath(job, new Path(args[2]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}

