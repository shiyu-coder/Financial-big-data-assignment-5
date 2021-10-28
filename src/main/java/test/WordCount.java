package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class WordCount {
    public static void main(String args[]) throws ClassNotFoundException, IOException, InterruptedException{

        //存放文本文件的目录
//        String in = "D:\\Hadoop\\code\\test_hadoop\\input\\shakespeare\\";
        String in = "hdfs://hadoop-master:9000/user/86137/input/shakespeare/";

        //词频统计结果的存放目录
//        String wordCount = "D:\\Hadoop\\code\\test_hadoop\\output\\rawCount\\";
        String wordCount = "hdfs://hadoop-master:9000/user/86137/output/rawCount/";

        //对词频统计结果进行排序后结果的存放目录
        String sort = "D:\\Hadoop\\code\\test_hadoop\\output\\sort\\";
//        String sort = "hdfs://hadoop-master:9000/user/86137/output/sort/";

//        String[] textList=new File(in).list();
        FileSystem fs = FileSystem.get(new Configuration());
        FileStatus[] fstates = fs.listStatus(new Path(in));//获取文件目录
        Path[] textList = FileUtil.stat2Paths(fstates);

        //排序前100条结果的存放目录
//        String topK = "D:\\Hadoop\\code\\test_hadoop\\output\\";
        String topK = "hdfs://hadoop-master:9000/user/86137/output/";

//        for(String textName:textList){
//            //词频统计+排序
//            if(Count.run(in + textName, wordCount + textName)){
//                Sort.run(wordCount + textName, sort + textName, topK + textName);
//            }
//        }
        for(Path p: textList){
            String textName = p.getName();
            if(Count.run(in + textName, wordCount + textName)){
                Sort.run(wordCount + textName, sort + textName, topK + textName);
            }
        }
        // 对所有词频统计结果合并并排序，得到所有文本的词频统计+排序结果
//        Combiner.run(wordCount + "*\\", topK + "rawCountAll");
        Combiner.run(wordCount + "*/", topK + "rawCountAll");
        Sort.run(topK + "rawCountAll", sort + "all", topK + "all");
    }
}
