package SaleCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outKey = new Text();
    private static final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        String type = split[4];
        if (!type.equals("4"))
            return;
        String time = split[2];
        // 用正则表达式匹配出时间中的年月日时分，格式为:2016-03-12 00:08:36
        time = time.replaceAll("^(\\d{4})-(\\d{2})-(\\d{2}) (\\d{2}):(\\d{2}):(\\d{2})$", "$1-$2-$3 $4:$5");
        outKey.set(time);
        context.write(outKey, one);
    }
}
