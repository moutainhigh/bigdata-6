package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立收件人表
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_receiver_lib")
public class JuXinLiReceiverInfo extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String bill_no;// 标示订单号
	private String name;// 收货人姓名
	private String phone_num_list;// 收货人电话号码，用逗号分隔
	private String amount;// 送货金额
	private String count;// 送货次数
}
