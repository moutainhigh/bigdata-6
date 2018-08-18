package com.shazhx.juxinli.response;

import com.shazhx.juxinli.entity.JuXinLiCellBehaviorInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * 聚信立�?�话行为分析
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiCellBehaviorResponse {
	private String phone_num;// 通话号码
	private List<JuXinLiBehaviorResponse> behavior;// 详细行为
	
	private JuXinLiCellBehaviorInfo info;
}
