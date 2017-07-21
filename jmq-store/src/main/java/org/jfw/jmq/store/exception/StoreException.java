package org.jfw.jmq.store.exception;

public class StoreException extends Exception{
	public static final int BASE_ERROR_CODE = 90000;
	
	/**
	 * great than TopicConfig.maxSurvivalTime
	 */
	public static final int TEMP_MESSAGE_TIMEOUT=90001;
	
	
	public static final int STORE_IS_SHUTDOWN = 90101;
	public static final int WRITE_RODO_ERROR = 90102;
	public static final int READ_RODO_ERROR = 90103;
	
	
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
