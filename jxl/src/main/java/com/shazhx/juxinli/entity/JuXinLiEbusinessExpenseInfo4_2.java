package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 
 * @author chenlu
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_ebusiness_expense_lib_4_2")
public class JuXinLiEbusinessExpenseInfo4_2 extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String bill_no;// 标示订单号
	private String trans_mth;// 汇总月份
	private String all_amount;// 总购物金额
	private String all_count;// 总购物次数
	private String all_category;// 本月商品品类
}
