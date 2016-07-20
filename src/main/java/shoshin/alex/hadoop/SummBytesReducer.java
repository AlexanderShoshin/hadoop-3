package shoshin.alex.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.*;
import shoshin.alex.hadoop.io.StorageWritable;

/**
 *
 * @author Alexander_Shoshin
 */
public class SummBytesReducer extends SummBytes<NullWritable, Text> {
    @Override
    public void reduce(Text key, Iterable<StorageWritable> values, SummBytesReducer.Context context)
            throws IOException, InterruptedException {
        StorageWritable bytesStorage = summBytes(values);
        double bytesAverage = (double)bytesStorage.getSumm() / bytesStorage.getCount();
        context.write(NullWritable.get(), new Text(String.format("%1$s,%2$s,%3$s", key, bytesAverage, bytesStorage.getSumm())));
    }
}