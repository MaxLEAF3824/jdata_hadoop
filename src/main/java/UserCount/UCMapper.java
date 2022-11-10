package UserCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UCMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        String userId = split[0];
        String time = split[2];
        // 用正则表达式匹配出时间中的年月日时分，格式为:2016-03-12 00:08:36
        time = time.replaceAll("^(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})$", "$1-$2-$3 $4");
        outKey.set(time);
        outValue.set(userId);
        context.write(outKey, outValue);
    }
}
