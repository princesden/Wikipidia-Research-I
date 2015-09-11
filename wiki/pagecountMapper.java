package wiki;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class pagecountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
static enum Wiki { NUM_OF_WIKI_PAGES, BAD_RECORD };
 
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    
	  	Text text = new Text();
	  	IntWritable intWritable = new IntWritable(1);
	  	
		String line = value.toString();
		
		String temp[] = line.split(" ");
				
	
		if (temp.length > 1 && temp[1].length() != 0) {
			
			context.getCounter(Wiki.NUM_OF_WIKI_PAGES).increment(1);
			
			String pagename = temp[1];
			String pageview = temp[2];
			int intpageview = Integer.parseInt(pageview);
			
			//for (String word : pagename.split("")) {
				if (pagename.length() > 0) {					
							
				text.set(pagename);
				intWritable.set(intpageview);
				context.write(text, intWritable);
		        }
		 // }
		}
			
		else { context.getCounter(Wiki.BAD_RECORD).increment(1);               
		
		}	
		
    }
  }

