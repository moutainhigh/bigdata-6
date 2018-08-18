package com.shazhx.juxinli.response;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.shazhx.juxinli.entity.JuXinLiCheckBlackInfo;

/**
 * 
 * @author chenlu
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiCheckBlackInfoResponse {
	private String phone_gray_score;
	private String contacts_class1_blacklist_cnt;
	private String contacts_class2_blacklist_cnt;
	private String contacts_class1_cnt;
	private String contacts_router_cnt;
	private String contacts_router_ratio;
	
	private JuXinLiCheckBlackInfo info;
}
