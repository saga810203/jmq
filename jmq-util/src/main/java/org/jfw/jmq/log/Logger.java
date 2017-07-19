package org.jfw.jmq.log;

public interface Logger
{
    boolean isEnableTrace();
	void trace(String message);
	void trace(String message,Throwable t);
	boolean isEnableDebug();
	void debug(String message);
	void dubug(String message,Throwable t);
	boolean isEnableInfo();
	void info(String message);
	void info(String message,Throwable t);
	boolean isEnableWarn();
	void warn(String message);
	void warn(String message,Throwable t);
	void error(String message);
	void error(String message,Throwable t);
	void fatal(String message);
	void fatal(String message,Throwable t);
	
	
}
