package com.mobanker.jieba;

/**
 * Created by liuxinyuan on 2017/12/22.
 */
public class Pair<K> {
    public K key;
    public Double freq = 0.0;

    public Pair(K key, double freq) {
        this.key = key;
        this.freq = freq;
    }

    @Override
    public String toString() {
        return "Candidate [key=" + key + ", freq=" + freq + "]";
    }

}
