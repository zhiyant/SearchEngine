
package org.example;

import io.PageFileWriter;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import utils.StoreUrl;
import utils.NewsData;
import utils.PreprocessUrl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.jsoup.Connection.Response;
import org.jsoup.nodes.Document;



public class MultiThreadCrawler {
    private static final String SPIDER_WEB_ROOT = "https://news.tongji.edu.cn";
    private static final int THREAD_COUNT = 9;
    private static final int TARGET_NEWS_COUNT = 20000;

    /** 不感兴趣的链接后缀。 */
    public static List<String> pageExtensionIgnoreList = Arrays.asList(
            ".png", ".jpg", ".mp4", ".css"
    );

    /** 不感兴趣的 href 前缀。 */
    public static List<String> pagePrefixIgnoreList = Arrays.asList(
            "https://", "http://", "mailto:", "ftp://", "#"
    );

    /** 不感兴趣的 href 内容关键字。 */
    public static List<String> pageContentIgnoreKeywords = Arrays.asList(
            "https://", "mailto:", "ftp://", "http://", "download.jsp", "javascript:"
    );
    public static Pattern hrefRegex = Pattern.compile("href=\".x?\"");

    public static void main(String[] args) throws InterruptedException, IOException {
        /** 爬虫线程队列。 */
        ArrayList<Thread> threads = new ArrayList<>();
        /** 文件写入器。创建后立即准备。 */
        final PageFileWriter fileWriter = new PageFileWriter("./result3.txt");
        /** 链接管理器。 */
        final StoreUrl linkStore = new StoreUrl();
        /** 已爬取的新闻数（所谓“已爬取”，实际是从拿到链接就算）。 */
        final AtomicInteger newsFetchedCounter = new AtomicInteger();
        /** 爬取结束信号。 */
        final Semaphore newsTargetReachedSemaphore = new Semaphore(THREAD_COUNT, true);
        /** 待爬取队列非空信号。 */
        final Semaphore linkQueueAvailableSemaphore = new Semaphore(1000000);
        /* 将首页加入进去。 */
        linkStore.addLink(SPIDER_WEB_ROOT);
        /* 放出一个信号。 */
        linkQueueAvailableSemaphore.release();
        /* 创建并启动爬虫线程。 */
        for (int i = 0; i < THREAD_COUNT; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {

                    /* ---------------- 爬虫爬取线程内容 开始 --------------- */
                    while(true){
                        try {
                            linkQueueAvailableSemaphore.acquire();// 获取信号。
                            // 在这里执行需要运行在协程上下文中的操作。
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        if (newsFetchedCounter.get() >= TARGET_NEWS_COUNT) { // 如果已经达到目标，就结束。
                            break;
                        }

                        String link = linkStore.get();
                        if (link == null) {
                            continue;
                        }

                        System.out.println(Thread.currentThread().getId() + ": fetching " + link);

                        Response response;
                        try {
                            response = Jsoup.connect(link)
                                    .followRedirects(false)
                                    .execute();
                        } catch (Exception e) {
                            System.out.println(Thread.currentThread().getId() + ": exception while fetching " + link + ".");
                            continue;
                        }

                        // 检查状态码。
                        int statusCode = response.statusCode();

                        if (statusCode != 200) {
                            System.out.println(Thread.currentThread().getId() + ": error while fetching " + link + ". code is " + statusCode);
                            continue;
                        }

                        Document document = null;
                        try {
                            document = response.parse();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        if (link.startsWith("https://news.tongji.edu.cn/info/")) {

                            /* ---------------- 新闻页面：读取内容 --------------- */

                            NewsData newsData = NewsData.toNewsData(document); // 提取内容。后续描述。
                            if (newsData != null) {
                                int newsCount = newsFetchedCounter.incrementAndGet();

                                System.out.println(Thread.currentThread().getId() + ": news " + newsCount + ": " + link);

                                if (newsCount <= TARGET_NEWS_COUNT) {
                                    newsData.url = link;
                                    try {
                                        fileWriter.writeLine(newsData.toString()); // 将爬取结果写入文件。
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }

                                if (newsCount >= TARGET_NEWS_COUNT) {
                                    newsTargetReachedSemaphore.release(); // 释放“爬取新闻数达到预期”信号。
                                }
                            }
                        }

                        /* -------------------- 爬取其他页面 ----------------- */
                        Elements aLink = document.getElementsByTag("a");
                        final PageFileWriter URLWriter = new PageFileWriter("./URLs/result"+Thread.currentThread().getId()+".txt");
                        try {
                            URLWriter.writeLine("🍎");
                            URLWriter.writeLine(link);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        for (Element aTag: aLink) {

                            String href = aTag.attr("href");

                            /** 排除不含关键字 */
                            for (String key : pageContentIgnoreKeywords) {
                                if(href.contains(key)){
                                    break;
                                }
                            }
                            /** 排除前缀 */
                            for (String key : pageExtensionIgnoreList) {
                                if(href.endsWith(key)){
                                    break;
                                }
                            }
                            /** 排除后缀 */
                            for (String key : pagePrefixIgnoreList) {
                                if(href.startsWith(key)){
                                    break;
                                }
                            }

                            href = PreprocessUrl.concatUrl(link, href);

                            try {
                                URLWriter.writeLine(href);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }

                            linkStore.addLink(href);
                            linkQueueAvailableSemaphore.release();
                        }
                    }
                    /* ---------------- 爬虫爬取线程内容 结束 --------------- */
                }

            });

            thread.start();
            threads.add(thread);
        }
        newsTargetReachedSemaphore.acquire(); // 阻塞当前线程，直到可以获得信号量

        /* 等待所有爬虫工作完毕。 */
        for (Thread thread : threads) {
            thread.join();
        }

        /* 关闭文件输出流。 */
        fileWriter.close();
    }


}






