package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiTransactionsCallsInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立运营商明细信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiTransactionsCallsResponse {
	private String update_time;// 更新时间
	private String place;// 通话时所在地
	private String other_cell_phone;// 通话号码
	private String start_time;// 通话�?始时�?
	private String cell_phone;// 本机号码
	private String init_type;// 呼叫类型
	private String call_type;// 通话类型
	private String use_time;//

	private JuXinLiTransactionsCallsInfo info;


	public JuXinLiTransactionsCallsInfo convertToTransactionsCallsInfo(JuXinLiTransactionsCallsResponse juXinLiTransactionsCallsResponse){
		JuXinLiTransactionsCallsInfo t = new JuXinLiTransactionsCallsInfo();
		if(juXinLiTransactionsCallsResponse != null){
			t.setCupdateTime(juXinLiTransactionsCallsResponse.getUpdate_time());
			t.setPlace(juXinLiTransactionsCallsResponse.getPlace());
			t.setOtherCellPhone(juXinLiTransactionsCallsResponse.getOther_cell_phone());
			t.setStartTime(juXinLiTransactionsCallsResponse.getStart_time());
			t.setCellPhone(juXinLiTransactionsCallsResponse.getCell_phone());
			t.setInitType(juXinLiTransactionsCallsResponse.getInit_type());
			t.setCallType(juXinLiTransactionsCallsResponse.getCall_type());
			t.setUseTime(juXinLiTransactionsCallsResponse.getUse_time());
		}
		return t;
	}
}
