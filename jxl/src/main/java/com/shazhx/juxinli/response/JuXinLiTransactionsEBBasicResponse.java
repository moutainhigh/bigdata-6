package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiTransactionsEBBasicInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 电商账户信息
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiTransactionsEBBasicResponse {
	private String update_time;// 更新时间
	private String email;// email地址
	private String cell_phone;// 账户手机号码
	private String security_level;// 安全级别
	private String level;// 会员级别
	private String is_validate_real_name;// 是否实名
	private String nickname;// 账户昵称
	private String register_date;// 注册时间
	private String real_name;// 真实姓名
	
	private JuXinLiTransactionsEBBasicInfo info;


	public JuXinLiTransactionsEBBasicInfo convertEBBasicInfo(JuXinLiTransactionsEBBasicResponse juXinLiTransactionsEBBasicResponse){
		JuXinLiTransactionsEBBasicInfo t = new JuXinLiTransactionsEBBasicInfo();
		if(juXinLiTransactionsEBBasicResponse != null){
			t.setBupdateTime(juXinLiTransactionsEBBasicResponse.getUpdate_time());
			t.setEmail(juXinLiTransactionsEBBasicResponse.getEmail());
			t.setCellPhone(juXinLiTransactionsEBBasicResponse.getCell_phone());
			t.setSecurityLevel(juXinLiTransactionsEBBasicResponse.getSecurity_level());
			t.setLevel(juXinLiTransactionsEBBasicResponse.getLevel());
			t.setIsValidateRealName(juXinLiTransactionsEBBasicResponse.getIs_validate_real_name());
			t.setNickname(juXinLiTransactionsEBBasicResponse.getNickname());
			t.setRegisterDate(juXinLiTransactionsEBBasicResponse.getRegister_date());
			t.setRealName(juXinLiTransactionsEBBasicResponse.getReal_name());
		}
		return t;
	}
}
