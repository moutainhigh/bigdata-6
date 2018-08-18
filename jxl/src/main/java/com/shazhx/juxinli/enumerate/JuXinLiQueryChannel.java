package com.shazhx.juxinli.enumerate;

/**
 * 聚信立查询Channel
 * 
 * @author songxiangwei
 * 
 */
public enum JuXinLiQueryChannel {
	ALL("all"), MOBILE("mobile"), E_BUSINESS("e_business");

	private String type;

	private JuXinLiQueryChannel(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return type;
	}
}
