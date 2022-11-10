package CountSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, Text, CountBean> {
    private Text outKey = new Text();
    private CountBean outValue = new CountBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        String id = split[1];
        String type = split[4];
        int sale = type.equals("4") ? 1 : 0;
        int brow = type.equals("1") ? 1 : 0;
        outKey.set(id);
        outValue.setSale(sale);
        outValue.setBrow(brow);

        context.write(outKey, outValue);
    }
}
