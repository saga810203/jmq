package org.jfw.jmq.log;

public class NoLogger implements Logger
{

	@Override
    public void trace(String message)
    {
	   	    
    }

	@Override
    public void trace(String message, Throwable t)
    {
	    
    }

	@Override
    public void debug(String message)
    {
	    
    }

	@Override
    public void dubug(String message, Throwable t)
    {
	    
    }

	@Override
    public void info(String message)
    {
	    
    }

	@Override
    public void info(String message, Throwable t)
    {
	    
    }

	@Override
    public void warn(String message)
    {
	    
    }

	@Override
    public void warn(String message, Throwable t)
    {
	    
    }

	@Override
    public void error(String message)
    {
	    
    }

	@Override
    public void error(String message, Throwable t)
    {
	    
    }

	@Override
    public void fatal(String message)
    {
	    
    }

	@Override
    public void fatal(String message, Throwable t)
    {
	    
    }

    public boolean isEnableTrace() {
        return false;
    }

    public boolean isEnableDebug() {
        return false;
    }

    public boolean isEnableInfo() {
        return false;
    }

    public boolean isEnableWarn() {
        return false;
    }

}
