package shoshin.alex.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Alexander_Shoshin
 */
public class AverageSummWritable implements Writable {
    private double average;
    private int summ;
    
    public AverageSummWritable() {
        this(0, 0);
    }
    
    public AverageSummWritable(double average, int summ) {
        this.average = average;
        this.summ = summ;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(average);
        out.writeInt(summ);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        average = in.readDouble();
        summ = in.readInt();
    }
    
    public double getAverage() {
        return average;
    }
    
    public int getSumm() {
        return summ;
    }
    
    @Override
    public boolean equals(Object object) {
        AverageSummWritable storage = (AverageSummWritable) object;
        return (storage.summ == summ) && (storage.average == average);
    }
    
    @Override
    public String toString() {
        return String.format("%1$s,%2$s", average, summ);
    }
}