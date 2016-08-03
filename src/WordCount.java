import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 * Created by Moritz on 01.07.2016.
 */
public class WordCount {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException{

            String text = value.toString();
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

            StringTokenizer t1 = new StringTokenizer(text, "!§$%&/()=?+-,'\"; ");

            while(t1.hasMoreTokens()) {

                String word = t1.nextToken();
                context.write(new Text(word), new Text(filename));

            }
        }
    }

    public static class WordCountReducer extends Reducer<Text ,Text ,Text ,Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws
                IOException, InterruptedException{

            String files = new String();

            for (Text value : values) {

                if(files.length() == 0) files = value.toString();
                if(!files.contains(value.toString())) {
                    files = files + ": " + value.toString();
                }
            }

            context.write(new Text(key), new Text(files));

        }
    }

    public static void main(String [] args) throws Exception {
        //create and initialize job object
        Job job = Job.getInstance();
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCount");

        //specify input and output directories
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        //specify a mapper
        job.setMapperClass(WordCountMapper.class);

        //specify a reducer and a combiner
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountReducer.class);

        //specify ouput types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //start job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
