package SaleCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

public class SCRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        String countInputPath = "/output/JData_Action_201603_extra_DR/part-r-00000";
        String countOutputPath = "/output/SaleCount/";
        runSaleCount(countInputPath, countOutputPath);
    }

    public static void runSaleCount(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SCRunner.class);
        job.setMapperClass(SCMapper.class);
        job.setReducerClass(SCReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Path output_p = new Path(outputPath);
        FileSystem fileSystem = output_p.getFileSystem(conf);
        if (fileSystem.exists(output_p))
            fileSystem.delete(output_p, true);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, output_p);

        job.waitForCompletion(true);
    }
}