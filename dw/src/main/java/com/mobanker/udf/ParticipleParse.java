package com.mobanker.udf;

import com.huaban.analysis.jieba.JiebaSegmenter;
import com.huaban.analysis.jieba.WordDictionary;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuxinyuan on 2017/12/22.
 */
public class ParticipleParse extends UDF {

    private List<String> stopWords = new ArrayList<>();

    public List<String> getStopWords() {
        return stopWords;
    }


    public void buildStopWords(String srcPath) {
        try {
            File file = new File(srcPath);
            BufferedReader br = new BufferedReader(new FileReader(file));
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
            WordDictionary instance = WordDictionary.getInstance();
            Path path1 = Paths.get("/fenci");
            List<String> list = new ArrayList<>();
//            instance.init(path1);


            JiebaSegmenter segmenter = new JiebaSegmenter();
            buildStopWords("/fenci2/stopword.dict");
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


        ParticipleParse participleParse = new ParticipleParse();
        String evaluate = participleParse.evaluate("要注意的是idx的数字不能大于有趣店表达式中()的个数上海公积金米线。");
        System.out.println(evaluate);


    }

}
