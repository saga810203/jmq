package org.jfw.jmq.store.exception;

public class StoreException extends Exception{
	public static final int BASE_ERROR_CODE = 90000;
	
	/**
	 * great than TopicConfig.maxSurvivalTime
	 */
	public static final int TEMP_MESSAGE_TIMEOUT=90001;
	
	private int code;

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public StoreException( int code ,String message, Throwable cause) {
		super(message, cause);
		this.code = code;
	}

	public StoreException(int code ,String message) {
		super(message);
		this.code = code;
	}

	public StoreException(int code ,Throwable cause) {
		super(cause);
		this.code = code;
	}
	
	
	
	

}
