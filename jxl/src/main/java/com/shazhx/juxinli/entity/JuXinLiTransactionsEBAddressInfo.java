package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 电商地址信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_transactions_eb_address_lib")
public class JuXinLiTransactionsEBAddressInfo extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String billNo;// 标示订单号
	private String aupdateTime;// 更新时间
	private String receiverAddr;// 收货地址
	private String province;// 收货省份
	private String city;// 收货城市
	private String receiverName;// 收货人
	private String receiverPhone;// 收货电话
	private String isDefaultAddress;// 是否为无效地址
}
