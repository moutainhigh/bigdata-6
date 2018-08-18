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
public class JuXinLiRecentNeedReqCallCntResponse {
	private String call_out_cnt;// 对该�?求的主叫总次�?
	private String call_in_cnt;// 对该�?求的被叫总次�?
}
