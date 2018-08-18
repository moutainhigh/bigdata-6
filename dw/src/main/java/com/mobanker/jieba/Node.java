package com.mobanker.jieba;

/**
 * Created by liuxinyuan on 2017/12/22.
 */
public class Node {
    public Character value;
    public com.huaban.analysis.jieba.Node parent;

    public Node(Character value, com.huaban.analysis.jieba.Node parent) {
        this.value = value;
        this.parent = parent;
    }
}
