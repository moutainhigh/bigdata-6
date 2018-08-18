package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立联系人区域汇总
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_contact_region_lib")
public class JuXinLiContactRegionInfo extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String billNo;// 标示订单号
	private String regionLoc;// 联系人的号码归属地
	private String regionUniqNumCnt;// 去重后的联系人号码数量
	private String regionCallInCnt;// 电话呼入次数
	private String regionCallOutCnt;// 电话呼出次数
	private String regionCallInTime;// 电话呼入总时间（秒）
	private String regionCallOutTime;// 电话呼出总时间（秒）
	private String regionAvgCallInTime;// 平均电话呼入时间（秒）
	private String regionAvgCallOutTime;// 平均电话呼出时间（秒）
	private String regionCallInCntPct;// 电话呼入次数百分比
	private String regionCallOutCntPct;// 电话呼出次数百分比
	private String regionCallInTimePct;// 电话呼入时间百分比
	private String regionCallOutTimePct;// 电话呼出时间百分比

}
