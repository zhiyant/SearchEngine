package qwq.test;

import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.SegToken;

import qwq.io.StopWords;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    private static HashSet<String> stop_words = new HashSet<String>((new StopWords()).set);

    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        JiebaSegmenter segmenter = new JiebaSegmenter();
        String sentences = "https://news.tongji.edu.cn/info/1002/83905.htm党委学研工部 2023-04-29同济要闻表彰学业榜样，聆听奋进故事。4月28日下午，同济大学“青春同学 卓越济梦”学业榜样风采展示暨2021-2022学年奖学金颁奖典礼在四平路校区大礼堂举行，校长郑庆华出席并讲话。校党委副书记彭震伟、相关职能部门负责人、奖学金捐赠方代表、获奖学生代表及2000余名大一新生参加了颁奖典";

        String[] lineSplit = sentences.split("\u0001");
        assert lineSplit.length == 5;

        String content = lineSplit[4];
        long contentOffset = 0;
        for (int i = 0; i < 4; i++){
            contentOffset += lineSplit[i].getBytes().length + "\u0001".getBytes().length;
        }

        System.out.println("contentOffset = " + contentOffset);
        List<SegToken> segList = segmenter.process(content, JiebaSegmenter.SegMode.INDEX);
        System.out.println("list length = " + segList.size());
        for (SegToken token: segList){
            if (stop_words.contains(token.word)){
                System.out.println("removed " + token.word);
            }
            System.out.println(token.word + " : " + String.valueOf(contentOffset + content.substring(0, token.startOffset).getBytes(StandardCharsets.UTF_8).length));
        }
    }

    // @Test
    // public void testMap(){
    //     HashMap<String, Long> tr = new HashMap<>();

    //     tr.put("fg", 2L);
    //     tr.put("he", 8L);
    //     tr.put("naa", 9L);

    //     tr.compute("fg", (k, v) -> v == null ? new Long(114) : v + 114);
    //     tr.compute("qwe", (k, v) -> v == null ? new Long(114) : v + 114);
    //     tr.compute("naa", (k, v) -> v == null ? new Long(114) : v + 114);
    //     tr.compute("qwe", (k, v) -> v == null ? new Long(114) : v + 514);


    //     assert tr.size() == 4;

    //     System.out.println(tr);
    // }
}
