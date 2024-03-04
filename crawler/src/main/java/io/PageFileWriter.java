package io;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
/**
 * save the file to local path
 */
public class PageFileWriter {

    private final File file;
    private final Lock lock; /** try to change it to mutex for multi-thread use */

    /**
     * init fuction
     * @param filepath
     */
    public PageFileWriter(String filepath) {
        this.file = new File(filepath);
        this.lock = new ReentrantLock();
    }
    private FileWriter fileWriter;

    public void writeLine(String line) throws IOException {
        write(line, "\n");
    }

    /**
     * write into the file
     *
     * @param line
     */
    public void write(String line, String divider) throws IOException {
        lock.lock();
        try{
            if(fileWriter == null) {
                fileWriter = new FileWriter(file, true);
            }
            fileWriter.write(line);
            if(divider != null) {
                fileWriter.write(divider);
            }
            fileWriter.flush();
        }finally {
            lock.unlock();
        }
    }

    /**
     * close file
     */
    public void close() throws IOException {
        lock.lock();
        try {
            if(fileWriter != null) {
                fileWriter.close();
                fileWriter = null;
            }
        } finally {
            lock.unlock();
        }

    }
}


/**
 * saving files into HDFS
 */


 

/**
 * import java.io.FileInputStream;
 * import java.io.IOException;
 * import java.io.InputStream;
 *
 * import org.apache.hadoop.conf.Configuration;
 * import org.apache.hadoop.fs.FileSystem;
 * import org.apache.hadoop.fs.Path;
 *
 * public class HdfsFileWriter {
 *     public static void main(String[] args) throws IOException {
 *         String localFilePath = "path/to/local/file.txt";
 *         String hdfsFilePath = "hdfs://localhost:9000/path/to/hdfs/file.txt";
 *
 *         // 创建一个Configuration对象
 *         Configuration conf = new Configuration();
 *
 *         // 获取到Hadoop的FileSystem对象
 *         FileSystem fs = FileSystem.get(conf);
 *
 *         // 从本地文件系统读取文件
 *         InputStream in = new FileInputStream(localFilePath);
 *
 *         // 在HDFS上创建一个输出流
 *         Path outFile = new Path(hdfsFilePath);
 *         fs.create(outFile);
 *
 *         // 将本地文件写入HDFS文件
 *         byte[] buffer = new byte[4096];
 *         int bytesRead = -1;
 *         while ((bytesRead = in.read(buffer)) != -1) {
 *             fs.append(outFile, buffer, 0, bytesRead);
 *         }
 *
 *         // 关闭流
 *         in.close();
 *         fs.close();
 *     }
 * }
 */