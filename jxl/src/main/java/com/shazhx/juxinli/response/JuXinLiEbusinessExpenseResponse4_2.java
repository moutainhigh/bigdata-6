package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiEbusinessExpenseInfo4_2;

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
public class JuXinLiEbusinessExpenseResponse4_2 {
	private String trans_mth;// 汇�?�月�?
	private String all_amount;// 总购物金�?
	private String all_count;// 总购物次�?
	private String[] all_category;// 本月商品品类
	
	private JuXinLiEbusinessExpenseInfo4_2 info;
}
