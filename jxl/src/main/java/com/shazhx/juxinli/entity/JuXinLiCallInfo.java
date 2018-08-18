package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立呼叫信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_call_lib")
public class JuXinLiCallInfo extends BaseEntity {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String bill_no;// 标记订单号
	private String phone_num;// 联系人的电话号码
	private String phone_num_loc;// 号码的归属地
	private String call_cnt;// 呼叫次数
	private String call_len;// 呼叫时间(分钟)
	private String call_out_cnt;// 主叫次数
	private String call_in_cnt;// 被叫次数
	private String sms_cnt;// 发送和接收短信次数
	private String trans_start;// 在呼叫记录里面最早出现的时间
	private String trans_end;// 在呼叫记录里面最晚出现的时间
}
