/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package shoshin.alex.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Alexander_Shoshin
 */
public class SummCountWritable implements Writable {
    private int summ;
    private int count;
    
    public SummCountWritable() {
        this(0, 0);
    }
    
    public SummCountWritable(int summ, int count) {
        this.summ = summ;
        this.count = count;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(summ);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        summ = in.readInt();
        count = in.readInt();
    }
    
    public int getSumm() {
        return summ;
    }
    
    public int getCount() {
        return count;
    }
    
    public void increaseSumm(int val) {
        summ += val;
    }
    
    public void increaseCount(int val) {
        count += val;
    }
    
    @Override
    public boolean equals(Object object) {
        SummCountWritable storage = (SummCountWritable) object;
        return (storage.summ == summ) && (storage.count == count);
    }
    
    @Override
    public String toString() {
        return String.format("%1$s,%2$s", summ, count);
    }
}