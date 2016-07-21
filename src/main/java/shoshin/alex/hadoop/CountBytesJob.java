package shoshin.alex.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import shoshin.alex.hadoop.io.AverageSummWritable;
import shoshin.alex.hadoop.io.SummCountWritable;

public class CountBytesJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
        conf.setClass("mapreduce.output.fileoutputformat.compress.codec", SnappyCodec.class, CompressionCodec.class);
        int res = ToolRunner.run(conf, new CountBytesJob(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("countBytes");
        job.setJarByClass(CountBytesJob.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AverageSummWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SummCountWritable.class);
        job.setMapperClass(ExtractBytesByIdMapper.class);
        job.setReducerClass(SummBytesReducer.class);
        job.setCombinerClass(SummBytesCombiner.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        int result = job.waitForCompletion(true) ? 0 : 1;
        System.out.println(job.getCounters().findCounter("Map errors", "WRONG_LOG_FORMAT"));
        return result;
    }
}