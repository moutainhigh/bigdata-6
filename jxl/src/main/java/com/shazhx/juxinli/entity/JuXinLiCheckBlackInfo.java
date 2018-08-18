package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 
 * @author chenlu
 *
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_check_black_info_lib")
public class JuXinLiCheckBlackInfo extends BaseEntity {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String billNo;// 订单类型
	private String phone_gray_score;
	private String contacts_class1_blacklist_cnt;
	private String contacts_class2_blacklist_cnt;
	private String contacts_class1_cnt;
	private String contacts_router_cnt;
	private String contacts_router_ratio;
}
