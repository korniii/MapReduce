import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankCalculatorReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double rank = 0;
        String linkPages = new String();

        for(Text value: values){

            if(value.toString().contains("!!!")){
                linkPages = value.toString().replace("!!!","");
            }
            else {
                String[] result = value.toString().split("\t");
                rank += Double.parseDouble(result[1]);
            }
        }

        double pageRank = 0.15 + 0.85 * rank;

        String result = pageRank+"\t"+linkPages;

        context.write(page, new Text(result));
    }
}
