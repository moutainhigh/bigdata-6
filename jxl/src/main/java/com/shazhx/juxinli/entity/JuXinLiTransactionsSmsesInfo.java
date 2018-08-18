package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 运营商短信信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_transactions_smses_lib")
public class JuXinLiTransactionsSmsesInfo extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String billNo;
	private String supdateTime;// 更新时间
	private String cellPhone;// 本机号码
	private String otherCellPhone;// 短信来往号码
	private String initType;// 短信来往方式
	private String startTime;// 短信来往时间
	private String place;// 短信来往时所在地
}
