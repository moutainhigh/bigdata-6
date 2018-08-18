package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiTransactionsSmsesInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 运营商短信信�?
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiTransactionsSmsesResponse {
	private String update_time;// 更新时间
	private String cell_phone;// 本机号码
	private String other_cell_phone;// 短信来往号码
	private String init_type;// 短信来往方式
	private String start_time;// 短信来往时间
	private String place;// 短信来往时所在地
	
	private JuXinLiTransactionsSmsesInfo info;



	public JuXinLiTransactionsSmsesInfo concertToTransactionsSmses(JuXinLiTransactionsSmsesResponse juXinLiTransactionsSmsesResponse){
		JuXinLiTransactionsSmsesInfo t = new JuXinLiTransactionsSmsesInfo();
		if(juXinLiTransactionsSmsesResponse != null){
			t.setCellPhone(juXinLiTransactionsSmsesResponse.getCell_phone());
			t.setOtherCellPhone(juXinLiTransactionsSmsesResponse.getOther_cell_phone());
			t.setInitType(juXinLiTransactionsSmsesResponse.getInit_type());
			t.setStartTime(juXinLiTransactionsSmsesResponse.getStart_time());
			t.setPlace(juXinLiTransactionsSmsesResponse.getPlace());
			t.setSupdateTime(juXinLiTransactionsSmsesResponse.getUpdate_time());
		}
		return t;
	}
}
