package shoshin.alex.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import shoshin.alex.hadoop.io.StorageWritable;

public class CountBytesJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CountBytesJob(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("countBytes");
        job.setJarByClass(CountBytesJob.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StorageWritable.class);
        job.setMapperClass(ExtractBytesByIdMapper.class);
        job.setReducerClass(SummBytesReducer.class);
        job.setCombinerClass(SummBytesCombiner.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int result = job.waitForCompletion(true) ? 0 : 1;
        System.out.println(job.getCounters().findCounter("Map errors", "WRONG_LOG_FORMAT"));
        return result;
    }
}