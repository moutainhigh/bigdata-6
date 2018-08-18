package com.shazhx.juxinli.entity;

import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 聚信立黑名单接口
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
@Table(name = "t_juxinli_black_lib")
public class JuXinLiBlackInfo extends BaseEntity {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String name;// 姓名

	private String idCard;// 身份证

	private String phone;// 手机号码

	private String channel;// 调用渠道

	private String categories;// 信用分类

	private String others;// 其他信用分类

	private String mobiles;// 登记手机号码

	private String crdate;// 创建时间

}
