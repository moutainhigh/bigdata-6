package com.shazhx.juxinli.response;

import java.lang.reflect.InvocationTargetException;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.apache.commons.beanutils.BeanUtils;
import com.shazhx.juxinli.entity.JuXinLiCheckInfo;

/**
 * 聚信立行为检�?
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiBehaviorCheckResponse {
	private String category;// �?查点类别
	private String check_point;// �?查项�?
	private String result;// �?查结�?
	private String evidence;// 证据

	private JuXinLiCheckInfo info;

	public JuXinLiCheckInfo convertToBehaviorCheck(JuXinLiBehaviorCheckResponse juXinLiBehaviorCheckResponse) throws IllegalAccessException, InvocationTargetException{
		JuXinLiCheckInfo t = new JuXinLiCheckInfo();
		if(juXinLiBehaviorCheckResponse != null){
			BeanUtils.copyProperties(juXinLiBehaviorCheckResponse, t);
			t.setCheckPoint(juXinLiBehaviorCheckResponse.getCheck_point());
		}
		return t;

	}
}
