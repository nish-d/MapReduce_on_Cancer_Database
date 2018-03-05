

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

public class ByAge {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String columns[]=line.split("\\|");
            String count=columns[3];
            String age=columns[0];
            String site=columns[9];
            String gender=columns[8];
            String year=columns[10];
            String type= site + " | " + year;
            System.out.println(type + " " + gender);
            Text category= new Text(type);
            if( !columns[0].equals("<1")){
                try{
                    context.write(category , new IntWritable(Integer.valueOf(count)));
                }catch(Exception e){
                    System.err.println(e.getMessage());
                }
            }



        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            IntWritable sum = new IntWritable(0);
            for (IntWritable val : values) {
                sum.set(sum.get()+ val.get());
            }
            context.write(key, sum);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //year wise distribution, calculates how many cases of different types of cancer were recorded in each year
        //Brain and Other Nervous System | 2000	127396
        //in the year 2000, 127396 cases were recorded of Brain & Other Nervous System Cancer
        //this is sum total of all age groups
        Job job = new Job(conf, "ByAge");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
