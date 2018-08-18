package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 算话上传
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_suanhua_shangchuan")
public class SuanHuaTradeUpLoad extends BaseEntity {

	/**
	 * Version
	 */
	private static final long serialVersionUID = 1L;

	private String batchNo;// 批次号

	private String name;// 姓名

	private String cardNo; // 身份证号

	private String cardType;// 身份证类型

	private String account;// 合同号

	private String type;// 上传类型

	private String tradeDate;// 逾期日期

	private String tradeAmt;// 逾期金额

	private String phone; // 手机号码

}
