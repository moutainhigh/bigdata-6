package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiContactListInfo;

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
public class JuXinLiContactListResponse {
	private String phone_num;// 号码
	private String phone_num_loc;// 号码归属�?
	private String contact_name;// 号码被标注的名称
	private String needs_type;// 被标注的名称的类�?
	private String call_cnt;// 通话次数
	private String call_len;// 通话时长(秒）
	private String call_out_cnt;// 呼出次数
	private String call_out_len;// 呼出时间(�?)
	private String call_in_cnt;// 呼入次数
	private String call_in_len;// 呼入时间(�?)
	private String p_relation;// 关系推测（未实现�?
	private String contact_1w;// �?近一周联系次�?
	private String contact_1m;// �?近一月联系次�?
	private String contact_3m;// �?近三月联系次�?
	private String contact_early_morning;// 凌晨联系次数
	private String contact_morning;// 上午联系次数
	private String contact_noon;// 中午联系次数
	private String contact_afternoon;// 晚上联系次数
	private String contact_night;// 深夜联系次数
	private String contact_all_day;// 是否全天联系
	private String contact_weekday;// 周中联系次数
	private String contact_weekend;// 周末联系次数
	private String contact_holiday;// 节假日联系次�?
	
	private JuXinLiContactListInfo info;
}
