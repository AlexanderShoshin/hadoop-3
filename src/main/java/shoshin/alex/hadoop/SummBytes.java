package shoshin.alex.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import shoshin.alex.hadoop.io.StorageWritable;

/**
 *
 * @author Alexander_Shoshin
 */
public class SummBytes<KEYOUT extends Object, VALUEOUT extends Object> extends Reducer<Text, StorageWritable, KEYOUT, VALUEOUT> {
    protected StorageWritable summBytes(Iterable<StorageWritable> values) {
        StorageWritable resultStorage = new StorageWritable();
        for (StorageWritable bytesStorage: values) {
            resultStorage.increaseSumm(bytesStorage.getSumm());
            resultStorage.increaseCount(bytesStorage.getCount());
        }
        return resultStorage;
    }
}
