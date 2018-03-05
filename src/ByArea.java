import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author rajani
 */
public class ByArea {

    public static class Map1 extends Mapper<LongWritable, Text, Text, DoubleWritable>{

        public void map(LongWritable key, Text value, Context context){
            String row[] = value.toString().split("[|]");
            String state=row[0];
            String count=row[4];
            String site=row[9];
            String race=row[7];
            String year=row[10];
            if(!site.equals("All Cancer Sites Combined") && !race.equals("All Races") && !year.equals("2010-2014"))
            try{
                context.write(new Text(state+ "|" + site + "|" + year+ "|")
                        , new DoubleWritable(Integer.parseInt(count)));
            }catch (Exception e){
                System.err.println(e.getMessage());
            }

        }

    }

    public static class Reduce1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
        //finds total number of cases of cancer of different sites in different states in all years
        private DoubleWritable total = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(DoubleWritable val: values){
                sum += val.get();
            }
            total.set(sum);
            context.write(key, total);
        }
    }

    public static class Map2
            extends Mapper<LongWritable, Text,Text, Text>{

        public void map(LongWritable key, Text value, Context context){
            String[] row = (value.toString()).split("\\|");
            String state=row[0].trim();
            String site=row[1].trim();
            String year=row[2].trim();
            String total=row[3].trim();
            try {
                DoubleWritable count = new DoubleWritable(Double.parseDouble(total));
                context.write(new Text(state+"|"),new Text(count+"|"+year));
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }

        }
    }

    // sort in descending , make rating as key and movie id as value

    public static class Reduce2 extends Reducer<Text, Text, Text, DoubleWritable>{
        //finds which site and state had the highest average number of cancer cases and which year
        String maxSite;
        double maxCount=0;
        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
            for (Text val : value){
                String[] countperyear=(val.toString()).split("\\|");
                double count=Double.parseDouble(countperyear[0]);
                if(count>maxCount){
                    maxCount=count;
                    maxSite=countperyear[1];
                }

            }
            context.write(new Text(key + "|" + maxSite), new DoubleWritable(maxCount));
        }


    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "chainingHw");
        job1.setJarByClass(ByArea.class);
        job1.setMapperClass(Map1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(DoubleWritable.class);

        job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean complete = job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "chainingH");
        if(complete){
            job2.setJarByClass(ByArea.class);
            job2.setMapperClass(Map2.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);



            job2.setReducerClass(Reduce2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);



            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}