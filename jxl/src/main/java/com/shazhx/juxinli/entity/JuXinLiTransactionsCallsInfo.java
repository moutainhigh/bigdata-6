package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立运营商明细信息 通话时长
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_transactions_calls_lib")
public class JuXinLiTransactionsCallsInfo extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String billNo;// 标示订单号
	private String cupdateTime;// 更新时间
	private String place;// 通话时所在地
	private String otherCellPhone;// 通话号码
	private String startTime;// 通话开始时间
	private String cellPhone;// 本机号码
	private String initType;// 呼叫类型
	private String callType;// 通话类型
	private String useTime;// 通话时长

}
