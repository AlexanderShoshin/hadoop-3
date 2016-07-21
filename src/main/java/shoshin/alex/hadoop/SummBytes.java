package shoshin.alex.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import shoshin.alex.hadoop.io.SummCountWritable;

/**
 *
 * @author Alexander_Shoshin
 */
public class SummBytes<KEYOUT extends Object, VALUEOUT extends Object> extends Reducer<Text, SummCountWritable, KEYOUT, VALUEOUT> {
    protected SummCountWritable summBytes(Iterable<SummCountWritable> values) {
        SummCountWritable resultStorage = new SummCountWritable();
        for (SummCountWritable bytesStorage: values) {
            resultStorage.increaseSumm(bytesStorage.getSumm());
            resultStorage.increaseCount(bytesStorage.getCount());
        }
        return resultStorage;
    }
}
