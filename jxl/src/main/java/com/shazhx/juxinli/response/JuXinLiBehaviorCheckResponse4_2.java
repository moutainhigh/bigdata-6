package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiBehaviorCheck;
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
public class JuXinLiBehaviorCheckResponse4_2 {
	private String check_point;// �?查项�?
	private String check_point_cn;// �?查项�?
	private String result;// �?查结�?
	private String evidence;// 证据
	private String score;// 0:无数�?, 1:通过, 2:不�?�过
	
	private JuXinLiBehaviorCheck info;
}
