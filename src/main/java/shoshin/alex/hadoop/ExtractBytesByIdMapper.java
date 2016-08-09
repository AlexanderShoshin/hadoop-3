package shoshin.alex.hadoop;

import java.io.IOException;
import net.sf.uadetector.ReadableUserAgent;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import shoshin.alex.hadoop.io.SummCountWritable;
import shoshin.alex.data.ServerLog;

public class ExtractBytesByIdMapper extends Mapper<LongWritable, Text, Text, SummCountWritable> {
    private static final Logger LOGGER = Logger.getLogger(ExtractBytesByIdMapper.class);
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {
            ServerLog log = ServerLog.parse(line);
            collectBrowsersStat(log, context);
            context.write(new Text(log.getId()), new SummCountWritable(log.getSentBytes(), 1));
        } catch (IllegalArgumentException exception) {
            LOGGER.error("Exception occured: " + exception.getMessage() + ", wrong line format in " + line);
            context.getCounter("Map errors", "WRONG_LOG_FORMAT").increment(1);
        }
    }
    
    private void collectBrowsersStat(ServerLog log, Mapper.Context context) {
        UserAgentStringParser parser = UADetectorServiceFactory.getResourceModuleParser();
        ReadableUserAgent browser = parser.parse(log.getBrowser());
        context.getCounter("Browser used", browser.getName()).increment(1);
    }
}