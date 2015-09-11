package wikisort;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class wikiInvertReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

 
  public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
	  
	  Text word = null;
	  
	  for (Text value : values) {
		  
		   word = value;
		}			
		
		
		context.write(key, new Text(word));
	}
}