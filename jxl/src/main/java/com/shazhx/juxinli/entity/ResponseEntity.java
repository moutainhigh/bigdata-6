package com.shazhx.juxinli.entity;

import java.io.Serializable;

public class ResponseEntity implements Serializable {
	private static final long serialVersionUID = -720807478055084231L;
	private String status;
	private String error;
	private String msg;
	private Object data;
	private String channel;
	private String transerialsId;
	private Boolean feeFlag;
	private Object AccessReportData;

	public Object getAccessReportData() {
		return this.AccessReportData;
	}

	public void setAccessReportData(Object accessReportData) {
		this.AccessReportData = accessReportData;
	}

	public Boolean getFeeFlag() {
		return this.feeFlag;
	}

	public void setFeeFlag(Boolean feeFlag) {
		this.feeFlag = feeFlag;
	}

	public String getTranserialsId() {
		return this.transerialsId;
	}

	public void setTranserialsId(String transerialsId) {
		this.transerialsId = transerialsId;
	}

	public ResponseEntity() {
	}

	public ResponseEntity(String status) {
		this.status = status;
	}

	public ResponseEntity(String status, String error) {
		this.status = status;
		this.error = error;
	}

	public ResponseEntity(String status, Object data) {
		this.status = status;
		this.data = data;
	}

	public ResponseEntity(String status, Object data, String channel) {
		this.status = status;
		this.data = data;
		this.channel = channel;
	}

	public ResponseEntity(String status, Object data, String channel, String transerialsId) {
		this.status = status;
		this.data = data;
		this.channel = channel;
		this.transerialsId = transerialsId;
	}

	public ResponseEntity(String status, String error, String msg) {
		this.status = status;
		this.error = error;
		this.msg = msg;
	}

	public ResponseEntity(String status, String error, String msg, String channel) {
		this.status = status;
		this.error = error;
		this.msg = msg;
		this.channel = channel;
	}

	public String getStatus() {
		return this.status;
	}

	public ResponseEntity setStatus(String status) {
		this.status = status;
		return this;
	}

	public String getError() {
		return this.error;
	}

	public ResponseEntity setError(String error) {
		this.error = error;
		return this;
	}

	public String getMsg() {
		return this.msg;
	}

	public ResponseEntity setMsg(String msg) {
		this.msg = msg;
		return this;
	}

	public Object getData() {
		return this.AccessReportData;
	}

	public ResponseEntity setData(Object data) {
		this.data = data;
		return this;
	}

	public String getChannel() {
		return this.channel;
	}

	public void setChannel(String channel) {
		this.channel = channel;
	}
}
