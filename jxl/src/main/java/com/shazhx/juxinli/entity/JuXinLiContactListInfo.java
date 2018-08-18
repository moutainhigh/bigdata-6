package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立联系人列表
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_contact_list_lib")
public class JuXinLiContactListInfo extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String billNo;// 订单类型
	private String phone_num;// 号码
	private String phone_num_loc;// 号码归属地
	private String contact_name;// 号码被标注的名称
	private String needs_type;// 被标注的名称的类型
	private String call_cnt;// 通话次数
	private String call_len;// 通话时长(秒）
	private String call_out_cnt;// 呼出次数
	private String call_out_len;// 呼出时间(秒)
	private String call_in_cnt;// 呼入次数
	private String call_in_len;// 呼入时间(秒)
	private String p_relation;// 关系推测（未实现）
	private String contact_1w;// 最近一周联系次数
	private String contact_1m;// 最近一月联系次数
	private String contact_3m;// 最近三月联系次数
	private String contact_early_morning;// 凌晨联系次数
	private String contact_morning;// 上午联系次数
	private String contact_noon;// 中午联系次数
	private String contact_afternoon;// 晚上联系次数
	private String contact_night;// 深夜联系次数
	private String contact_all_day;// 是否全天联系
	private String contact_weekday;// 周中联系次数
	private String contact_weekend;// 周末联系次数
	private String contact_holiday;// 节假日联系次数
}
