package shoshin.alex.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.*;
import shoshin.alex.hadoop.io.AverageSummWritable;
import shoshin.alex.hadoop.io.SummCountWritable;

/**
 *
 * @author Alexander_Shoshin
 */
public class SummBytesReducer extends SummBytes<Text, AverageSummWritable> {
    @Override
    public void reduce(Text key, Iterable<SummCountWritable> values, SummBytesReducer.Context context)
            throws IOException, InterruptedException {
        SummCountWritable bytesStorage = summBytes(values);
        double bytesAverage = (double)bytesStorage.getSumm() / bytesStorage.getCount();
        context.write(key, new AverageSummWritable(bytesAverage, bytesStorage.getSumm()));
    }
}