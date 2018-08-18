package com.mobanker.udf;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.WordDictionary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuxinyuan on 2017/12/22.
 */
public class ParticipleParse2 extends UDF {

    private static List<String> stopWords = new ArrayList<>();
    private static WordDictionary instance;
    private static JiebaSegmenter segmenter = new JiebaSegmenter();

    public List<String> getStopWords() {
        return stopWords;
    }

    static {
        try {
            String srcPath = "/user/hive/jar/ext_dict_t.dict";   //用户自定义词典
            instance = WordDictionary.getInstance();
         //   instance.loadUserDict(getReader(srcPath));
            buildStopWords("/user/hive/jar/stopword.dict");  //停用词典
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static BufferedReader getReader(String uri) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        org.apache.hadoop.fs.Path readPath = new org.apache.hadoop.fs.Path(uri);
        FSDataInputStream readStream = fs.open(readPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(readStream.getWrappedStream(), "utf-8"));
        return br;
    }

    public static void buildStopWords(String srcPath) {
        try {

            BufferedReader br = getReader(srcPath);
            String str;
            while ((str = br.readLine()) != null) {
                stopWords.add(str);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String evaluate(String str) {
        try {
            List<String> list = new ArrayList<>();
            List<String> strings = segmenter.sentenceProcess(str);
            for (String s : strings) {
                if (!stopWords.contains(s.toLowerCase())) {
                    list.add(s);
                }
            }

            return list.toString();

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        ParticipleParse2 participleParse = new ParticipleParse2();
        String evaluate = participleParse.evaluate("要注意的是idx的数字不能大于有趣店表达式中()的个数上海公积金米线。");
        System.out.println(evaluate);
    }

}
