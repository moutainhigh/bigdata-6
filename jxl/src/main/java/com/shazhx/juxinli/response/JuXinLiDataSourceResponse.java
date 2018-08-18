package com.shazhx.juxinli.response;

import java.lang.reflect.InvocationTargetException;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.apache.commons.beanutils.BeanUtils;
import com.shazhx.juxinli.entity.JuXinLiDataSourceInfo;

/**
 * 聚信立绑定数据源
 * 
 * @author songxiangwei
 * 
 */
@Data
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class JuXinLiDataSourceResponse {

	private String key;// 数据源标�?
	private String name;// 数据源名�?
	private String account;// 账号名称
	private String category_name;// 数据类型
	private String category_value;// 数据类型名称
	private String status;// 数据有效�?
	private String reliability;// 数据可靠�?
	private String binding_time;// 绑定时间

	private JuXinLiDataSourceInfo info;



	public JuXinLiDataSourceInfo convertToJuXinLiDataSource(JuXinLiDataSourceResponse juXinLiDataSourceResponse) throws IllegalAccessException, InvocationTargetException{
		JuXinLiDataSourceInfo datasourceInfo = new JuXinLiDataSourceInfo();
		if(juXinLiDataSourceResponse != null){
			BeanUtils.copyProperties(juXinLiDataSourceResponse, datasourceInfo);
			datasourceInfo.setCategoryName(juXinLiDataSourceResponse.getCategory_name());
			datasourceInfo.setCategoryValue(juXinLiDataSourceResponse.getCategory_value());
			datasourceInfo.setBindingTime(juXinLiDataSourceResponse.getBinding_time());
		}
		return datasourceInfo;
	}
}
