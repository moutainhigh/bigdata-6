package com.shazhx.juxinli.response;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.shazhx.juxinli.entity.JuXinLiReceiverInfo;

/**
 * 聚信立收件人�?
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiReceiverResponse {
	private String name;// 收货人姓�?
	private String[] phone_num_list;// 收货人电话号码，用�?�号分隔
	private String amount;// 送货金额
	private String count;// 送货次数
	
	private JuXinLiReceiverInfo info;
}
