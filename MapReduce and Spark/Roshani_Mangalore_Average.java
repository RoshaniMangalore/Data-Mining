import java.io.IOException;
import java.util.regex.Pattern;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Roshani_Mangalore_Average {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
        
        private Text event = new Text();
    private IntWritable page_count = new IntWritable();   
        
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
	  
	  String [] tokens = value.toString().split(",");
	if (tokens[0].equals("id"))
		return;
else{  
      String eventname = tokens[3];
	  
	  String events=eventname.toLowerCase().trim().replaceAll("['-]", "").replaceAll("[\\p{Punct}]"," ").replaceAll("( )+"," ").trim();
	  String no_of_pages=tokens[18];
	  
	  

	if (events!=null && events!=" " && events!="	" && events.matches(".*[a-z0-9A-Z].*"))
 { 	
        event.set(events);
        page_count.set(Integer.parseInt(no_of_pages));
	context.write(event, page_count);
      }
	     
      }
    }}

  
  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,Text> {
    private Text result = new Text();
	private String avg=new String();
private String favg=new String();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
  float sum = 0;
	  int count = 0;
      for (IntWritable val : values) {
        count+=1;
		sum += val.get();
      }
	  {
		
		avg=String.format("%.3f", sum/count);
		result.set(count+"\t"+avg);
	
		context.write(key,result); 
	
	  }
         }
  }

 public static void main(String [] args) throws Exception{
	Configuration conf = new Configuration();
	String [] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
	if(otherArgs.length < 2){
		System.err.println("Average");
		System.exit(2);
	}
	Job job = Job.getInstance(conf,"Roshani_Mangalore_Average");
	job.setJarByClass(Roshani_Mangalore_Average.class);
	job.setMapperClass(TokenizerMapper.class);
	job.setReducerClass(IntSumReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	for(int i=0;i<otherArgs.length-1;++i){
	FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	}
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
	System.exit(job.waitForCompletion(true) ? 0:1);
	
}}

