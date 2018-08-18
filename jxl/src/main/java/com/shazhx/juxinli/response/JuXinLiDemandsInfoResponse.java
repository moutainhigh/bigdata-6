package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiDomandsInfo;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立需求信�?
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiDemandsInfoResponse {
	private String demands_name;// �?求名�?
	private String demands_call_out_cnt;// 对该�?求的主叫总次�?
	private String demands_call_in_cnt;// 对该�?求的被叫总次�?
	private String demands_call_out_time;// 对该�?求的主叫总时长（秒）
	private String demands_call_in_time;// 对该�?求的被叫总时长（秒）
	
	private JuXinLiDomandsInfo info;
}
