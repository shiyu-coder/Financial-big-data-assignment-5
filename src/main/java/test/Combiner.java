package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class Combiner {

    public static class ConbinMap extends Mapper<Object, Text, Text, LongWritable> {

        LongWritable outKey = new LongWritable();
        Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                String element = st.nextToken();
                if (Pattern.matches("\\d+", element)) {
                    outKey.set(Integer.parseInt(element));
                } else {
                    outValue.set(element);
                }
            }

            context.write(outValue, outKey);
        }

    }

    @SuppressWarnings("deprecation")
    public static boolean run(String in,String out) throws IOException, ClassNotFoundException, InterruptedException{

        Configuration conf = new Configuration();

        Job job = new Job(conf, "Combiner");
        job.setJarByClass(Count.class);
        job.setMapperClass(ConbinMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> other_args = new ArrayList<String>();

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        return job.waitForCompletion(true);
    }
}
