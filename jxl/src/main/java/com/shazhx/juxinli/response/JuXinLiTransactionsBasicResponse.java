package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiTransactionsBasicInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立运营商基本信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiTransactionsBasicResponse {
	private String update_time;// 更新时间
	private String cell_phone;// 本机号码
	private String idcard;// 证件�?
	private String reg_time;// 登记时间
	private String real_name;// 姓名
	
	private JuXinLiTransactionsBasicInfo info;


	public JuXinLiTransactionsBasicInfo convert(JuXinLiTransactionsBasicResponse juXinLiTransactionsBasicResponse){
		JuXinLiTransactionsBasicInfo t = new JuXinLiTransactionsBasicInfo();
		if(juXinLiTransactionsBasicResponse != null){
			t.setCellPhone(juXinLiTransactionsBasicResponse.getCell_phone());
			t.setIdcard(juXinLiTransactionsBasicResponse.getIdcard());
			t.setBupdateTime(juXinLiTransactionsBasicResponse.getUpdate_time());
			t.setRegTime(juXinLiTransactionsBasicResponse.getReg_time());
			t.setRealName(juXinLiTransactionsBasicResponse.getReal_name());
		}
		return t;

	}
}
