package shoshin.alex.hadoop;

import shoshin.alex.hadoop.io.SummCountWritable;

public class MyThread extends Thread {
    private SummCountWritable bytes;
    public MyThread(SummCountWritable bytes) {
        this.bytes = bytes;
    }
    @Override
    public void run()	//Этот метод будет выполнен в побочном потоке
    {
        int i = bytes.getCount();
        bytes.increaseCount(i + 10);
    }
}
