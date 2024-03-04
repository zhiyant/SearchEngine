package qwq.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Scanner;
import java.util.StringTokenizer;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;

import qwq.io.StopWords;
import qwq.io.TokenInfoClient;


public class App {
    private static final int RESULT_NUM = 10;
    private static final int SINGLE_RESULT_NUM = 3;
    private static final int CONTEXT_LENGTH = 30;  // 上下文显示长度
    private static final String INDEX_ROOT = "/final/SearchEngine/index";  // 一级索引文件的根目录 假设多个一级索引文件都在该目录下
    private static final String SECONDARY_INDEX_PATH = "/final/SearchEngine/secondary_index/part-r-00000";  // 二级索引文件路径
    private static final String ORIGIN_ROOT = "/final/SearchEngine/web";  // 原文件路径
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(SECONDARY_INDEX_PATH);
        FileSystem fs = FileSystem.get(conf);
        String key = "";

        try (Scanner scanner = new Scanner(System.in)) {
            System.out.print("Please input a keyword:\n> ");
            key = scanner.nextLine().trim().toLowerCase(Locale.ROOT);
        }

        search(key, fs, path, conf);

    }

    public static void search(String key, FileSystem fs, Path path, Configuration conf) throws Exception {
        JiebaSegmenter segmenter = new JiebaSegmenter();
        HashSet<String> stop_words = new HashSet<String>((new StopWords()).set);
        HashSet<String> keywords = new HashSet<String>();
        ArrayList<TokenInfoClient> tokenInfoClientList = new ArrayList<TokenInfoClient>(); 

        StringTokenizer tokenizer = new StringTokenizer(key);
        while (tokenizer.hasMoreTokens()){
            // String word = tokenizer.nextToken();
            // if (!word.isEmpty() && !word.equals(" "))
            //     keywords.add(word);

            keywords.add(tokenizer.nextToken());
        }

        // keywords.add(key);
        List<SegToken> segList = segmenter.process(key, JiebaSegmenter.SegMode.INDEX);
        for (SegToken segToken: segList){
            String word = segToken.word;
            if (!word.isEmpty() && !word.equals(" ") && !stop_words.contains(word))
                keywords.add(segToken.word);
        }
        System.out.println(keywords);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))){
            String line = "";
            int flag = 0;
            while((line = reader.readLine()) != null){
                String[] lineSplit = line.split("\t");
                String token = lineSplit[0];
                String entry = lineSplit[1];

                if(keywords.contains(token)){
                    String[] entrySplit = entry.split("@");
                    Path fileName = new Path(INDEX_ROOT, entrySplit[0]);
                    long offset = Long.valueOf(entrySplit[1]);
                    
                    // search_index(fs, fileName, offset);

                    String[] termInfos = get_index_info(fs, fileName, offset);
                    double idf = Double.valueOf(termInfos[0]);
                    String[] singleInfos = termInfos[1].split(";");

                    // 把信息存起来
                    for(int i = 0; i < singleInfos.length && i < RESULT_NUM; i++){
                        String[] singleInfoSplit = singleInfos[i].split("@");
                        double rank = Double.valueOf(singleInfoSplit[0]);
                        long line_offset = Long.valueOf(singleInfoSplit[1]);
                        String origin_filename = singleInfoSplit[2];
                        String offsets = singleInfoSplit[3];
                        
                        // 在列表中是否有同一网页
                        Optional<TokenInfoClient> tokenInfoClient =
                            tokenInfoClientList.stream()
                                                .filter(
                                                    t -> t.line_offset.equals(line_offset) && t.origin_filename.equals(origin_filename))
                                                .findFirst();
                        if (tokenInfoClient.isPresent()){
                            // 如果列表中有该文件，该文件的信息加上tfidf和对应的offsets
                            tokenInfoClient.get().add(rank * idf, offsets);
                        } else {
                            // 如果没有，加到列表中
                            tokenInfoClientList.add(new TokenInfoClient(rank * idf, line_offset, origin_filename, offsets));
                        }
                    }

                    flag++;
                    if (flag == keywords.size())
                        break;
                }
            }

            tokenInfoClientList.sort(TokenInfoClient::compareTo);
            int i = 0;
            for (TokenInfoClient tokenInfoClient: tokenInfoClientList){
                if (++i > RESULT_NUM) break;    

                System.out.println(String.format("tf-idf = %.6f", tokenInfoClient.tfidf));
                // <rank>@<line_offset>@<filename>@[<word_offset>]
                show_single_info(
                    fs, tokenInfoClient.line_offset,
                    tokenInfoClient.origin_filename,
                    tokenInfoClient.offsets
                );
            }

        } catch (FileNotFoundException e) {
            System.out.println(key + ": secondary index not exists");
        }

    }

    public static String[] get_index_info(FileSystem fs, Path fileName, long offset) throws Exception {
        try (FSDataInputStream in = fs.open(fileName)){
            in.seek(offset);

            String lineIndex = in.readLine();
            String[] termInfos = lineIndex.split("\t")[1].split("\\|");
                        
            return termInfos;
        } catch (FileNotFoundException e) {
            System.out.println(fileName + ": index file not exists");
        }
        return null;
    }

    public static void show_single_info(FileSystem fs, Long line_offset, String origin_filename, String offsets) throws Exception {
        try (FSDataInputStream in2 = fs.open(new Path(ORIGIN_ROOT, origin_filename))){
            // for(int i = 0; i < singleInfos.length && i < RESULT_NUM; i++){

            String[] offsets_inline = offsets.split(":");

            in2.seek(line_offset);
            BufferedReader reader2 = new BufferedReader(new InputStreamReader(in2, "UTF-8"));
            String url = "";
            String source = "";
            String date = "";
            String title = "";
            int charsRead;
            while ((charsRead = reader2.read()) != '\u0001') {
                url += String.valueOf((char)charsRead);
            }
            while ((charsRead = reader2.read()) != '\u0001') {
                source += String.valueOf((char)charsRead);
            }
            while ((charsRead = reader2.read()) != '\u0001') {
                date += String.valueOf((char)charsRead);
            }
            while ((charsRead = reader2.read()) != '\u0001') {
                title += String.valueOf((char)charsRead);
            }
            
            System.out.println("found in " + url + " title: " + title + " source: " + source + " date: " + date);

            // for (int j = 0; j < offsets_inline.length && j < SINGLE_RESULT_NUM; j++){
            //     long wordOffset = Long.valueOf(offsets_inline[j]);
            //     if (wordOffset == -1){
            //         continue;
            //     }
            //     in2.seek(line_offset + wordOffset);
            //     reader2 = new BufferedReader(new InputStreamReader(in2, "UTF-8"));
            //     char[] buf = new char[CONTEXT_LENGTH];
            //     reader2.read(buf);
            //     String result = new String(buf);

            //     System.out.println("\n\t..." + result + "...");
            // }
            in2.skip(1);

            String content = reader2.readLine();
            for (int j = 0; j < offsets_inline.length && j < SINGLE_RESULT_NUM; j++){
                int wordOffset = Integer.valueOf(offsets_inline[j]);
                if (wordOffset == -1){
                    continue;
                }

                int begin = Math.max(0, wordOffset - CONTEXT_LENGTH);
                int end = Math.min(content.length(), wordOffset + CONTEXT_LENGTH);
                
                String result = content.substring(begin, end);
                System.out.println("\n\t..." + result + "...");
            }
        } catch (FileNotFoundException e) {
            System.out.println("can not find origin file");
        }
    }
}
