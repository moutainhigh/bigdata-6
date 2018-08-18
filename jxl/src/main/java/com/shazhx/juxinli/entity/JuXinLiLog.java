package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立Log
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_log")
public class JuXinLiLog extends BaseEntity {
	private static final long serialVersionUID = 7315786710871129186L;
	/** 订单号(调用方的流水号) **/
	private String merchantSerial;
	/** 流水号-对应每次请求 **/
	private String transerialsId;
	/** 渠道 **/
	private Integer channel;
	/** 服务类型 **/
	private String serviceType;
	/** 调用状态 **/
	private Integer requestStatus;
	/** 收费标记 **/
	private Integer feeFlag;
	/** 请求文本 **/
	private String requestText;
	/** 响应文本 **/
	private String responseText;
}
