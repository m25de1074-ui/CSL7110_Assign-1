import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountQ9 {
  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

      // Convert line to lowercase and remove punctuation
      String cleanLine = value.toString()
                              .toLowerCase()
                              .replaceAll("[^a-zA-Z ]", " ");

      StringTokenizer itr = new StringTokenizer(cleanLine);

      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  public static class IntSumReducer
       extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {

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
    // 32 MB  -> 33554432
    // 64 MB  -> 67108864
    // 128 MB -> 134217728
    conf.setLong(
      "mapreduce.input.fileinputformat.split.maxsize",
      134217728   // <-- currently 128 MB
    );

    Job job = Job.getInstance(conf, "Word Count Q9");

    job.setJarByClass(WordCountQ9.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // ===== EXECUTION TIME MEASUREMENT =====
    long startTime = System.currentTimeMillis();

    boolean success = job.waitForCompletion(true);

    long endTime = System.currentTimeMillis();

    System.out.println(
      "Total Execution Time: " + (endTime - startTime) + " ms"
    );

    System.exit(success ? 0 : 1);
  }
}

