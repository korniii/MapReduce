import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankCalculatorMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String [] values = value.toString().split("\t");

        String ausgabe = new String();
        String page = values[0];
        double rank = Double.parseDouble(values[1]);
        int numberOfLinks = values.length - 2;

        for(int i = 2; i < values.length; i++){
            context.write(new Text(values[i]), new Text(page + "\t" +(rank/numberOfLinks)));
        }

        for(int i = 2; i < values.length; i++){
            if(ausgabe.isEmpty()){
                ausgabe = values[i];
            }
            else {
                ausgabe = ausgabe+"\t"+values[i];
            }


        }

        context.write(new Text(page), new Text("!!!"+ausgabe));

    }
}
