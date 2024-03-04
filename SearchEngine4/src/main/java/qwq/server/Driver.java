package qwq.server;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;


public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.exit(ToolRunner.run(conf, new Driver(), args));
    }

    public int run(String[] args) throws Exception {
        // 输入文件
        Path inputPath = new Path(args[0]);

        // 一级索引文件（文本文件）。格式：
        // <token>@<filename>	<position>;<position>;<position>
        Path indexPath = new Path(args[1]);

        // 二级索引文件（文本文件）。格式：
        // <token>		<idf>$[<tf>@<tf_index_filename>@<tf_index_offset>]
        Path secondaryIndexPath = new Path(args[2]);
    
        Configuration conf = getConf();
        try (FileSystem fs = FileSystem.get(conf)) {
            long totalURLCount = 0;
            FileStatus status = fs.getFileStatus(inputPath);
            if (status.isFile()){
                totalURLCount = getURLCount(fs, inputPath);
            } else if (status.isDirectory()){
                FileStatus[] files = fs.listStatus(inputPath);
                for (FileStatus file: files){
                    Path childPath = file.getPath();
                    totalURLCount += getURLCount(fs, childPath);
                }
            }
            if (totalURLCount == 0)
                return 0;
            conf.setLong("totalURLCount", totalURLCount);
      
            if (!fs.exists(indexPath) && !runJob1(inputPath, indexPath))    System.exit(1);
            if (!fs.exists(secondaryIndexPath) && !runJob2(indexPath, secondaryIndexPath)) System.exit(1);
        }
        return 0;
    }

    private boolean runJob1(Path inputPath, Path outputPath)
      throws IOException, InterruptedException, ClassNotFoundException {
        Job job1 = Job.getInstance(getConf(), "first index");
        job1.setJarByClass(PosTF.class);

        job1.setMapperClass(PosTF.Map.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setCombinerClass(PosTF.Combine.class);
        
        job1.setReducerClass(PosTF.Reduce.class);
        job1.setNumReduceTasks(128);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath);

        return job1.waitForCompletion(true);
    }

    private boolean runJob2(Path inputPath, Path outputPath)
      throws IOException, InterruptedException, ClassNotFoundException {
      
        Job Job2 = Job.getInstance(getConf(), "secondary index");
        Job2.setJarByClass(SecondaryIndex.class);

        Job2.setMapperClass(SecondaryIndex.Map.class);
        Job2.setMapOutputKeyClass(Text.class);
        Job2.setMapOutputValueClass(Text.class);

        Job2.setReducerClass(SecondaryIndex.Reduce.class);
        // Job2.setNumReduceTasks(NUM_REDUCE_TASKS);
        Job2.setOutputKeyClass(Text.class);
        Job2.setOutputValueClass(Text.class);
        
        // Job2.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(Job2, inputPath);
        FileOutputFormat.setOutputPath(Job2, outputPath);

        return Job2.waitForCompletion(true);
    }

    private int getURLCount(FileSystem fs, Path path){
      try (LineNumberReader reader = new LineNumberReader(new InputStreamReader(fs.open(path)))){
          FileStatus fileStatus = fs.getFileStatus(path);
          long fileLength = fileStatus.getLen();
          reader.skip(fileLength);
          return reader.getLineNumber();
      } catch (Exception e) {
          System.out.println("origin file read error");
      }
      return -1;
  }
}
