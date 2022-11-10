package DuplicateRemoval;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DRMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        if (value.toString().contains("_id"))
            return;
        context.write(value, NullWritable.get());
    }
}
