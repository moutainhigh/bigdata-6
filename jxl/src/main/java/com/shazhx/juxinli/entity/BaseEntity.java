package com.shazhx.juxinli.entity;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Id;

import lombok.Data;

@Data
public class BaseEntity implements Serializable{

	/**
	 * serialVersionUID:TODO锛堢敤涓�鍙ヨ瘽鎻忚堪杩欎釜鍙橀噺琛ㄧず浠�涔堬級
	 *
	 * @since 1.0.0
	 */
	
	private static final long serialVersionUID = 4089285837632643882L;

	public static final String CREATE_DATE_PROPERTY_NAME = "createTime";// "鍒涘缓鏃ユ湡"灞炴�у悕绉�
	public static final String MODIFY_DATE_PROPERTY_NAME = "updateTime";// "淇敼鏃ユ湡"灞炴�у悕绉�
	@Id
	protected Long id;// ID
	protected Date createTime;// 鍒涘缓鏃ユ湡
	protected String createUser;// 鍒涘缓浜�
	protected Date updateTime;// 淇敼鏃ユ湡
	protected String updateUser;// 淇敼浜�
	
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		BaseEntity other = (BaseEntity) obj;
		if (id == null || other.getId() == null) {
			return false;
		} else {
			return (id.equals(other.getId()));
		}
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}
}
