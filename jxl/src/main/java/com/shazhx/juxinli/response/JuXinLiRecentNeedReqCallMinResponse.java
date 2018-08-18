package com.shazhx.juxinli.response;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立近期需�?
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiRecentNeedReqCallMinResponse {
	private String call_out_time;// 对该�?求的主叫总时长（秒）
	private String call_in_time;// 对该�?求的被叫总时长（秒）
}
