package qwq.server;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

public class SecondaryIndex {
    
    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String word = value.toString().split("\t")[0];
            String index_filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            String indexInfo = index_filename + "@" + key.toString();

            context.write(new Text(word), new Text(indexInfo));
        }
    }

    public static class Reduce
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text indexInfo: values){
                context.write(key, indexInfo);
            }
        }
    }
}
