package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立送货地址列表
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_deliver_address_lib")
public class JuXinLiDeliverAddressInfo extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String bill_no;// 标示订单号
	private String receiver_bill_no;// 收件人订单号
	private String address;// 收货地址
	private String lng;// 经度
	private String lat;// 纬度
	private String predict_addr_type;// 地址类型
	private String begin_date;// 开始送货时间
	private String end_date;// 结束送货时间
	private String total_amount;// 总送货金额
	private String total_count;// 总送货次数
}
