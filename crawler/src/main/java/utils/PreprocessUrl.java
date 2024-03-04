package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PreprocessUrl {

    // static声明变量的作用：
    public static String ROOT_LINK = "https://news.tongji.edu.cn";

    /**
     * filter the NEWS page
     * @param
     * @return
     */
    //public boolean isNewsData(){
//        return this.link.startsWith("https://news.tongji.edu.cn/info/");
//    }
//    private static String mergeDoubleSlashes(String format) {
//        return ROOT_LINK.replace("//", "/").replace("https:/", "https://");
//    }

    public static String concatUrl (String currentUrl, String url) {

        // StringBuilder可变，String不可变
        String[] parentSegments = currentUrl.substring("https://".length()).split("/");
        StringBuilder parentBuilder = new StringBuilder("https://");

        //  标记 ：url是网页的根链接
        boolean siteRootAttached = false;
        if (parentSegments.length == 1) {
            parentBuilder.append(parentSegments[0]);

        }

        for (String seg : parentSegments) {
            if (seg == parentSegments[parentSegments.length - 1]) {
                break;
            }
            else if (!siteRootAttached){
                parentBuilder.append(seg);
                siteRootAttached = true;
            }
            else {
                parentBuilder.append(String.format("/%s", seg));
            }
        }


        String realParent = parentBuilder.toString();

        // 修改成静态方法？
        String pureUrl = String.format("%s/%s",realParent, url);
        pureUrl =pureUrl.replace("//", "/").replace("https:/", "https://");
        pureUrl = pureUrl.substring("https://".length());

        String[] sourceUrlSegments = pureUrl.split("/");
        List<String> resultUrlSegments = new ArrayList<String>();

        for (String seg:sourceUrlSegments) {
            if (Objects.equals(seg, ".")) {
                break; // return @ forEach方法的具体作用是：
            }
            else if (Objects.equals(seg, "..")) {
                if (!resultUrlSegments.isEmpty()) {
                    resultUrlSegments.remove(resultUrlSegments.size() - 1);
                }
            }
            else {
                resultUrlSegments.add(seg);
            }
        }

        StringBuilder resultBuilder = new StringBuilder("https://");
        siteRootAttached = false;
        for ( String seg : resultUrlSegments) {
            if (seg.isEmpty()) {
                continue;
            }
            else if ( !siteRootAttached ) {
                //
                resultBuilder.append(seg);
                siteRootAttached = true;
            }
            else {
                resultBuilder.append(String.format("/%s", seg));
            }

        }
        return resultBuilder.toString();
    }



}
