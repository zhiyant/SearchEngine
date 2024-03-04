package utils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StoreUrl {
    /** 队列存储待爬链接 */
    private Queue<String> linkQue;
    /** 哈希表存储已爬取链接 */
    public HashSet<String> linkCrawled = null;
    /** 互斥锁 */
    private ReentrantLock lock;
    public StoreUrl(){
        this.linkQue = new LinkedList();
        this.linkCrawled = new HashSet<String>();
        this.lock = new ReentrantLock();
    }

    /**
     * get url from the queue
     */
    public String get() {
        String link;
        lock.lock();
        link = linkQue.poll();
        lock.unlock();
        return link;
    }

    /**
     * find if url in hashset or not
     */
    public boolean addLink(String link) {
        lock.lock();
        if(!linkCrawled.contains(link)) {
            linkCrawled.add(link);
            linkQue.add(link);
            lock.unlock();
            return true;
        }
        lock.unlock();
        return false;
    }


}

