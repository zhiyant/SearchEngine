package utils;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;



/**
 * 新闻信息。
 */
public class NewsData{
        public String url = "";
        public String source = "";
        public String date = "";
        public String title = "";
        public String content = "";

        private static final String DIVIDER = "\001";

        @Override
        public String toString() {
            return String.format("%s%s%s%s%s%s%s%s%s", url,DIVIDER,source,DIVIDER,date,DIVIDER,title,DIVIDER,content);
        }


        public static NewsData toNewsData(Document doc) {
                NewsData result = new NewsData();

                Element content = doc.getElementsByClass("content").first();
//                System.out.println(doc);

                Element title = doc.getElementsByTag("h3").first();
                result.title = title.text();


                Element sourceAndDateContainer = content.getElementsByTag("i").first();
                System.out.println("sourceAndDateContainer的内容："+sourceAndDateContainer.text());
                String[] sourceAndDateSegments = sourceAndDateContainer.text().split(" 浏览");
                System.out.println("sourceAndDateContainer"+sourceAndDateContainer.text());


                System.out.println(sourceAndDateContainer.text());

                try {
                        String source = sourceAndDateSegments[0].split("：")[1];
                        result.date = sourceAndDateSegments[1];
                } catch (Exception e) {

                }

                /* 获取新闻的内容。 */
                Element newsContentContainer = content.getElementsByClass("v_news_content").first();
                result.content = newsContentContainer.text().replace("\n", "  ").replace("\r", "  ");

                if (!result.title.isEmpty() && !result.content.isEmpty() && !result.date.isEmpty())
                {
                        return result;
                } else {
                        return null;
                }


        }


}/** TODO: 改成扩展Document类的静态方法 */

