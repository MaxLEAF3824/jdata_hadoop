package CountSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, CountBean, Text> {
    private CountBean outKey = new CountBean();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        outKey.setSale(Integer.parseInt(split[1]));
        outKey.setBrow(Integer.parseInt(split[2]));
        outValue.set(split[0]);
        context.write(outKey, outValue);
    }
}
