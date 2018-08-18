package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiRecentNeedInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * 聚信立近期需�?
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiRecentNeedResponse {
	private List<JuXinLiDemandsInfoResponse> demands_info;
	private String req_type;// �?求类�?
	private JuXinLiRecentNeedReqCallCntResponse req_call_cnt;// 次数
	private JuXinLiRecentNeedReqCallMinResponse req_call_min;// 时间
	private String req_mth;// �?求发生的月份
	
	private JuXinLiRecentNeedInfo info;
}
