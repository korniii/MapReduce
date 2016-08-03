import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WikiPageLinksReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        String pageLinks = "1.0";


        for(Text text : values){
            pageLinks = pageLinks+"\t"+text.toString();
        }

        context.write(key, new Text(pageLinks));

    }
}
