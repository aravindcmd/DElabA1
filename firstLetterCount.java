import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class first {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String s = value.toString();                                  // Convert varible doc to String and assign to String S
      s = s.toUpperCase();      
      String f = new String();                                     // init f that will hold first letters later                                   
      String[] arr = s.split(" ");                                 // split s after every word by delimiter space
      ArrayList<String> list = new ArrayList<String>();           
      for (String str : arr){                                     // this forloop will remove any extra double spaces in the input, this is important
                                                                  // to avoid stringindex out of bounds error.
          if (!str.equals("")){               
            list.add(str);
          }
      }
      arr = list.toArray(new String[list.size()]);





        for(int i = 0; i<arr.length;i++){
            char c = arr[i].charAt(0);
            f+=" "+c;                                            // convert the input array to a single string so that we can feed to stringTokenizer
            // String sub = arr[i].substring(1);
        }

      StringTokenizer itr = new StringTokenizer(f);             // the stringTokenizer tokens each letter
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(first.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
