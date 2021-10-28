package test;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text word = new Text();
    private Set<String> stopWordList = new HashSet<String>();
    private Set<String> puncWordList = new HashSet<String>();

    protected void setup(Context context){
        // 停词文件路径
        Path stopWordFile = new Path("D:\\Hadoop\\code\\test_hadoop\\input\\stop-word-list.txt");
//        Path stopWordFile = new Path("hdfs://hadoop-master:9000/user/86137/input/stop-word-list.txt");
        // 标点符号文件路径
        Path puctFile = new Path("D:\\Hadoop\\code\\test_hadoop\\input\\punctuation.txt");
//        Path puctFile = new Path("hdfs://hadoop-master:9000/user/86137/input/punctuation.txt");
        readWordFile(stopWordFile, puctFile);
    }

//    private void readWordFile(Path stopWordFile, Path pucFile){
//        Configuration conf = new Configuration();
//        try{
//            FileSystem fs = FileSystem.get(URI.create(stopWordFile.toString()),conf);
//            InputStream is = fs.open(new Path(stopWordFile.toString()));
//            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
//            String stopWord = null;
//            while ((stopWord = reader.readLine()) != null) {
//                stopWordList.add(stopWord);
//            }
//        }catch (IOException ioe){
//            ioe.printStackTrace();
//        }
//        try{
//            FileSystem fs = FileSystem.get(URI.create(pucFile.toString()),conf);
//            InputStream is = fs.open(new Path(pucFile.toString()));
//            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
//            String pucWord = null;
//            while ((pucWord = reader.readLine()) != null) {
//                puncWordList.add(pucWord);
//            }
//        }catch (IOException ioe){
//            ioe.printStackTrace();
//        }
//    }

    private void readWordFile(Path stopWordFile, Path pucFile) {
        try {
            BufferedReader fis1 = new BufferedReader(new FileReader(stopWordFile.toString()));
            String stopWord = null;
            while ((stopWord = fis1.readLine()) != null) {
                stopWordList.add(stopWord);
            }
        } catch (IOException ioe) {
            System.err.println("Exception while reading stop word file '"
                    + stopWordFile + "' : " + ioe.toString());
        }
        try{
            BufferedReader fis2 = new BufferedReader(new FileReader(pucFile.toString()));
            String pucWord = null;
            while ((pucWord = fis2.readLine()) != null) {
                puncWordList.add(pucWord);
            }
        } catch (IOException ioe) {
            System.err.println("Exception while reading punc word file '"
                    + pucFile + "' : " + ioe.toString());
        }
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            token = token.toLowerCase();
            //
            Pattern pattern = Pattern.compile("[\\d]");
            Matcher matcher = pattern.matcher(token);
            token = matcher.replaceAll("").trim();
//            token = token.replaceAll("[\\pP\\p{Punct}]","");
            if (token.length() >= 3 && !stopWordList.contains(token) && !puncWordList.contains(token)) {
//            if (token.length() >= 3 && !stopWordList.contains(token)) {
                for(String puncWord: puncWordList){
                    token = token.replace(puncWord.substring(1), "");
                }
                if(token.length() >= 3){
                    word.set(token);
                    context.write(word, new LongWritable(1));
                }
            }
        }
    }
}
