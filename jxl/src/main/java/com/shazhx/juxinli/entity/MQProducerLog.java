package com.shazhx.juxinli.entity;

/**
 * @packageName: com.mobanker.analytics.entity
 * @description: 聚信立mq消息队列生产者日志
 * @author: zhuosh
 * @DATE: 2016/3/22
 */

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_amq_producer_msg")
public class MQProducerLog extends BaseEntity{

    //添加任务的主机IP
    private String addIp;

    //添加的内容
    private String msgContent;

    //当前日期的订单号
    private String merchantSerial;

    //错误的内容
    private String errorMsg;

}
