package qwq.server;
import qwq.io.StopWords;
import qwq.io.TokenInfoMap;
import qwq.io.TokenInfoReduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
// import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;


public class PosTF {

    public static class Map 
      extends Mapper<LongWritable, Text, Text, Text>{
    
        private JiebaSegmenter segmenter = new JiebaSegmenter();
        private static HashSet<String> stop_words = new HashSet<String>((new StopWords()).set);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            String lineOffset = key.toString();
            // 用来存单词对应的累加rank、各个位置的行内offset
            HashMap<String, TokenInfoMap> tokenInfoMap = new HashMap<>();
            
            String[] lineSplit = value.toString().split("\u0001");
            assert lineSplit.length == 6;

            String title = lineSplit[3];
            String content = lineSplit[4];
            float pagerank = Float.parseFloat(lineSplit[5]);

            // 处理标题里的词
            List<SegToken> segList = segmenter.process(title, JiebaSegmenter.SegMode.INDEX);
            for (SegToken segToken: segList){
                String word = segToken.word;
                // 停用词处理
                if (stop_words.contains(word)){
                    continue;
                }
                // 凡是出现在标题里的都rank+100，不考虑标题里的多次出现
                // 标题里的wordOffset设为-1
                tokenInfoMap.put(word, new TokenInfoMap(100L, "-1"));
            }

            // 处理正文的词
            segList = segmenter.process(content, JiebaSegmenter.SegMode.INDEX);
            long totalWordCount = segList.size();
            for (SegToken segToken: segList){
                String word = segToken.word;
                // 停用词处理
                if (stop_words.contains(word)){
                    continue;
                }
                String wordOffset = String.valueOf(segToken.startOffset);
                // 出现在正文的词，每个rank+1
                // 正文的wordOffset，以字符串拼接形式添加单词出现的多个位置
                tokenInfoMap.compute(word, (k, v) -> v == null ? new TokenInfoMap(1L, wordOffset) : v.add(1L, wordOffset));
            }

            // 遍历map，输出
            for (HashMap.Entry<String, TokenInfoMap> entry: tokenInfoMap.entrySet()) {
                double rank = (double) entry.getValue().rank / totalWordCount * pagerank;
                context.write(
                    new Text(entry.getKey()),
                    new Text(String.format("%.6f@%s@%s@%s", 
                        rank, lineOffset, filename, entry.getValue().offset))
                );
            }
        }
    }
    public static class Combine 
      extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<TokenInfoReduce> tokenInfoList = new ArrayList<TokenInfoReduce>();

            for(Text val: values){
                String[] valSplit = val.toString().split("@", 2);
                double rank = Double.valueOf(valSplit[0]);
                tokenInfoList.add(new TokenInfoReduce(rank, valSplit[1]));
            }
            // 含有token的网页数
            int url_count_with_token = tokenInfoList.size();
            
            String[] tokenInfoStr = new String[url_count_with_token];
            int i = 0;
            for (TokenInfoReduce tokenInfo: tokenInfoList) {
                tokenInfoStr[i++] = tokenInfo.toString();
            }
            Text outValue = new Text(String.format("%d", url_count_with_token) + "|" + String.join(";", tokenInfoStr));

            context.write(key, outValue);            
        }
    }

    public static class Reduce 
      extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 总网页数
            long total_url_count = context.getConfiguration().getLong("totalURLCount",1);

            ArrayList<TokenInfoReduce> tokenInfoList = new ArrayList<TokenInfoReduce>();
            ArrayList<Integer> array_count_with_token = new ArrayList<Integer>();

            for(Text val: values){
                System.out.println(val);
                String[] valSplit1 = val.toString().split("\\|",2);
                int count_with_token = Integer.valueOf(valSplit1[0]);
                String[] valSplit2 = valSplit1[1].toString().split(";");
                for(String tmp:valSplit2){
                    String[] valSplit3 = tmp.toString().split("@", 2);
                    double rank = Double.valueOf(valSplit3[0]);
                    tokenInfoList.add(new TokenInfoReduce(rank, valSplit3[1]));
                }
                array_count_with_token.add(new Integer(count_with_token));
            }

            // 含有token的网页数
            int url_count_with_token = 0;
            for(int i:array_count_with_token)
                url_count_with_token += i;
            int tokenInfoListsize = tokenInfoList.size();
            tokenInfoList.sort(TokenInfoReduce::compareTo);
            
            double idf = Math.log((double) total_url_count / url_count_with_token);

            String[] tokenInfoStr = new String[tokenInfoListsize];
            int i = 0;
            for (TokenInfoReduce tokenInfo: tokenInfoList) {
                tokenInfoStr[i++] = tokenInfo.toString();
            }
            Text outValue = new Text(String.format("%.6f", idf) + "|" + String.join(";", tokenInfoStr));

            context.write(key, outValue);
        }
    }
}
