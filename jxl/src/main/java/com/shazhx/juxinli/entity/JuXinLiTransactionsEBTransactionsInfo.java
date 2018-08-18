package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 电商交易明细
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_transactions_eb_transactions_lib")
public class JuXinLiTransactionsEBTransactionsInfo extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String billNo;// 标示订单号
	private String tupdateTime;// 更新时间
	private String transTime;// 交易时间
	private String totalPrice;// 交易总额
	private String receiverAddr;// 收货地址
	private String receiverName;// 收货人
	private String receiverCellPhone;// 收货电话
	private String paymentType;// 付款方式
	private String productPrice;// 商品单价
	private String productCnt;// 商品数量
	private String productName;// 商品名称
	private String deliveryType;// 快递方式
	private String deliveryFee;// 快递费用
	private String isSuccess;// 数据获取是否成功



}
