package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiEbusinessExpenseInfo;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立电商月消费
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiEbusinessExpenseResponse {
	private String trans_mth;// 汇�?�月�?
	private String owner_amount;// 本人购物金额
	private String owner_count;// 本人购物次数
	private String family_amount;// 本人+同住人购物金�?
	private String family_count;// 本人+同住人购物次�?
	private String others_amount;// 非本人和家庭购物的购物金�?
	private String others_count;// 非本人和家庭购物的购物次�?
	private String all_amount;// 总购物金�?
	private String all_count;// 总购物次�?
	
	private JuXinLiEbusinessExpenseInfo info;
}
