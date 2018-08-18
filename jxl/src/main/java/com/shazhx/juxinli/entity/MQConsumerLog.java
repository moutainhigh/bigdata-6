package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @packageName: com.mobanker.analytics.entity
 * @description: 聚信立mq队列  消费记录
 * @author: zhuosh
 * @DATE: 2016/3/22
 */


@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_amq_consumer_msg")
public class MQConsumerLog extends BaseEntity{

    //订单号
    private String merchantSerial;

    //ip
    private String ip;


    //错误日志
    private String errorMsg;


    //消费的内容
    private String msgContent;

}
