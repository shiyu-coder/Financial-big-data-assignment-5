package test;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;


public class WordCount {

    public static class WCMapper
            extends Mapper<LongWritable, Text, Text, LongWritable>{

        static List<String> li = new ArrayList<String>();

        @Override
        protected void setup(Context context)throws IOException, InterruptedException {
            super.setup(context);
            String uri="hdfs://localhost:9000/user/86137/input/stop-word-list.txt";
            Configuration conf=new Configuration();
            FileSystem fs=FileSystem.get(URI.create(uri),conf);
            InputStream in=null;
            try{
                in=fs.open(new Path(uri));
                List<String> lines = IOUtils.toString(in);
            }finally{
                IOUtils.closeStream(in);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = StringUtils.split(line, " ");
            for(String word: words){
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }

    public static class WCReducer
            extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            long count = 0;
            for(LongWritable value: values){
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job wcJob = Job.getInstance(conf);
        wcJob.setJarByClass(WordCount.class);
        wcJob.setMapperClass(WCMapper.class);
        wcJob.setReducerClass(WCReducer.class);
        wcJob.setOutputKeyClass(Text.class);
        wcJob.setOutputValueClass(LongWritable.class);
        wcJob.setMapOutputKeyClass(Text.class);
        wcJob.setMapOutputValueClass(LongWritable.class);
        wcJob.addCacheFile(new URI("hdfs://localhost:9000/user/86137/input/stop-word-list.txt"));
        FileInputFormat.addInputPath(wcJob, new Path(otherArgs[0] + "/shakespeare"));
        FileOutputFormat.setOutputPath(wcJob, new Path(otherArgs[1]));
        System.exit(wcJob.waitForCompletion(true) ? 0 : 1);
    }
}
