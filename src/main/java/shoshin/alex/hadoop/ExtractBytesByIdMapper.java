package shoshin.alex.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import shoshin.alex.hadoop.io.StorageWritable;
import shoshin.alex.data.ServerLog;

/**
 *
 * @author Alexander_Shoshin
 */
public class ExtractBytesByIdMapper extends Mapper<LongWritable, Text, Text, StorageWritable> {
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {
            ServerLog log = ServerLog.parse(line);
            context.write(new Text(log.getId()), new StorageWritable(log.getSentBytes(), 1));
        } catch (IllegalArgumentException exception) {
            context.getCounter("Map errors", "WRONG_LOG_FORMAT").increment(1);
        }
    }
}