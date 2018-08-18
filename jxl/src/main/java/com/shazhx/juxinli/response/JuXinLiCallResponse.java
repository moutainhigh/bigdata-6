package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiCallInfo;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立呼叫信�?
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiCallResponse {
	private String phone_num;// 联系人的电话号码
	private String phone_num_loc;// 号码的归属地
	private String call_cnt;// 呼叫次数
	private String call_len;// 呼叫时间(分钟)
	private String call_out_cnt;// 主叫次数
	private String call_in_cnt;// 被叫次数
	private String sms_cnt;// 发�?�和接收短信次数
	private String trans_start;// 在呼叫记录里面最早出现的时间
	private String trans_end;// 在呼叫记录里面最晚出现的时间
	
	private JuXinLiCallInfo info;
}
