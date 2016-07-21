package shoshin.alex.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.*;
import org.junit.Before;
import org.junit.Test;
import shoshin.alex.hadoop.io.AverageSummWritable;
import shoshin.alex.hadoop.io.SummCountWritable;

/**
 *
 * @author Alexander_Shoshin
 */
public class MRUnitCountBytesTest {
    private MapDriver<LongWritable, Text, Text, SummCountWritable> mapDriver;
    private ReduceDriver<Text, SummCountWritable, Text, SummCountWritable> combineDriver;
    private ReduceDriver<Text, SummCountWritable, Text, AverageSummWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, SummCountWritable, Text, AverageSummWritable> mapReduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, SummCountWritable, Text, AverageSummWritable> mapCombineReduceDriver;
    private String[] dataSet = {"ip1 - - [24/Apr/2011:05:45:58 -0400] \"GET /~strabal/grease/photo2/910-4.jpg HTTP/1.1\" 200 4600 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"",
                                "ip1 - - [24/Apr/2011:05:50:18 -0400] \"GET /~strabal/grease/photo8/T926-5.jpg HTTP/1.1\" 200 5400 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\"",
                                "ip32 - - [24/Apr/2011:05:54:48 -0400] \"GET /apache/manual/content-negotiation.html HTTP/1.1\" 200 22 \"-\" \"Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)\""};
    
    @Before
    public void setUp() {
        Mapper mapper = new ExtractBytesByIdMapper();
        Reducer combiner = new SummBytesCombiner();
        Reducer reducer = new SummBytesReducer();
        mapDriver = new MapDriver<LongWritable, Text, Text, SummCountWritable>();
        mapDriver.setMapper(mapper);
        combineDriver = new ReduceDriver<Text, SummCountWritable, Text, SummCountWritable>();
        combineDriver.setReducer(combiner);
        reduceDriver = new ReduceDriver<Text, SummCountWritable, Text, AverageSummWritable>();
        reduceDriver.setReducer(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapCombineReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, combiner);
    }
    
    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(dataSet[0]));
        mapDriver.withOutput(new Text("ip1"), new SummCountWritable(4600, 1));
        mapDriver.runTest();
    }
    
    @Test
    public void testCombiner() throws IOException {
        List<SummCountWritable> byteStorages = new ArrayList<SummCountWritable>();
        byteStorages.add(new SummCountWritable(12, 1));
        byteStorages.add(new SummCountWritable(50, 1));
        byteStorages.add(new SummCountWritable(30, 1));
        combineDriver.withInput(new Text("ip1"), byteStorages);
        combineDriver.withOutput(new Text("ip1"), new SummCountWritable(92, 3));
        combineDriver.runTest();
    }
    
    @Test
    public void testReducer() throws IOException {
        List<SummCountWritable> byteStorages = new ArrayList<SummCountWritable>();
        byteStorages.add(new SummCountWritable(20, 2));
        byteStorages.add(new SummCountWritable(30, 1));
        byteStorages.add(new SummCountWritable(50, 2));
        reduceDriver.withInput(new Text("ip1"), byteStorages);
        reduceDriver.withOutput(new Text("ip1"), new AverageSummWritable(20.0,100));
        reduceDriver.runTest();
    }
    
    @Test
    public void testMapReduce() throws IOException {
        testMapReduceDriver(mapReduceDriver);
    }
    
    @Test
    public void testMapCombineReduce() throws IOException {
        testMapReduceDriver(mapCombineReduceDriver);
    }
    
    private void testMapReduceDriver(MapReduceDriver driver) throws IOException {
        driver.withInput(new LongWritable(1), new Text(dataSet[0]));
        driver.withInput(new LongWritable(1), new Text(dataSet[1]));
        driver.withInput(new LongWritable(1), new Text(dataSet[2]));
        driver.withOutput(new Text("ip1"), new AverageSummWritable(5000.0,10000));
        driver.withOutput(new Text("ip32"), new AverageSummWritable(22.0,22));
        driver.runTest();
    }
}