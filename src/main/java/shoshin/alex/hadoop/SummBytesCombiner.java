package shoshin.alex.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import shoshin.alex.hadoop.io.StorageWritable;

/**
 *
 * @author Alexander_Shoshin
 */
public class SummBytesCombiner extends SummBytes<Text, StorageWritable> {
    @Override
    public void reduce(Text key, Iterable<StorageWritable> values, SummBytesCombiner.Context context)
            throws IOException, InterruptedException {
        context.write(key, summBytes(values));
    }
}