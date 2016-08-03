import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

public class RankOrderingMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String [] values = value.toString().split("\t");

        context.write(new FloatWritable(Float.parseFloat(values[1])), new Text(values[0]));

    }
}
