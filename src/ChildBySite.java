

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ChildBySite {

    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        public HashMap<String, String> map=new HashMap<>();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String columns[]=line.split("\\|");
            String age=columns[0];
            String count=columns[4];
            String site=columns[9];
            String year=columns[10];
            Text category= new Text(age+"|"+site);
            map.put(site, age);
            if( !columns[0].equals("<1")){
                try{
                    context.write(category , new IntWritable(Integer.valueOf(count)));
                }catch(Exception e){
                    System.err.println(e.getMessage());
                }
            }



        }
    }

    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            IntWritable sum = new IntWritable(0);
            for (IntWritable val : values) {
                sum.set(sum.get()+ val.get());
            }
            context.write(new Text(key+"|"), sum);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //Calculates total number of cancer cases of age group 0-14 and 0-19, over the years 2009-2014
        //0-14|Soft Tissue|	14803
        //this means 14803 cases of soft tissue cancer has been detected in age 0-14 age group from 2009 to 2014
        Job job = new Job(conf, "ByAge");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);

        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
