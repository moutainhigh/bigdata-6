package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @packageName: com.mobanker.analytics.entity
 * @description: 聚信立用户的信息实体类
 * @author: zhuosh
 * @DATE: 2016/2/23
 */

@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name="t_juxinli_userinfo")
public class JuXinLiUserInfo extends BaseEntity {

    /***手机号*/
    private String mobile;

    /***身份证*/
    private String idCard;

    /***用户姓名*/
    private String name;

    /***用户所有信息采集状态   0 为采集   1采集成功  2 采集失败  3不需要再次采集*/
    private Integer queryStatusAll;

    /***用户部分信息采集  0 未采集  1采集成功  2采集失败  3不需要再次采集*/
    private Integer queryStatusPart;

    /***站点类型*/
    private String webSiteType;

    /***数据的操作状态  0 表示不在消息队列中  1表示在消息队列中*/
    private Integer operationStatus;
}
