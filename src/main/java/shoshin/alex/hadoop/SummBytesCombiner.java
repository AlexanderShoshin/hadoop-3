package shoshin.alex.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import shoshin.alex.hadoop.io.SummCountWritable;

/**
 *
 * @author Alexander_Shoshin
 */
public class SummBytesCombiner extends SummBytes<Text, SummCountWritable> {
    @Override
    public void reduce(Text key, Iterable<SummCountWritable> values, SummBytesCombiner.Context context)
            throws IOException, InterruptedException {
        context.write(key, summBytes(values));
    }
}