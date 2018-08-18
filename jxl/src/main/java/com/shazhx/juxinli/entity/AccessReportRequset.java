package com.shazhx.juxinli.entity;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @packageName: com.mobanker.analytics.request
 * @description:聚信立用户请求参�?
 * @author: zhuosh
 * @DATE: 2016/2/23
 */

@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class AccessReportRequset {

    private static final long serialVersionUID = 1L;

    private String name;// 姓名
    private String idCard;// 身份�?
    private String phone;// 手机�?

    private String token;// token
    private String channel;// 渠道
    private String webSiteType; //网点
    private List<String> modules; // 模块列表
}
