package org.jfw.jmq.store.command;

import java.util.ServiceLoader;

public abstract class CommandFactory {
	private static final CommandFactory[]  cfs = new CommandFactory[255];
	
	public static Command build(byte b ){
		return cfs[b & 0xff].create();		
	}
	public static CommandFactory get(byte b){
		return cfs[b & 0xff];
	}
	
	public abstract byte getOrderCode();
	
	public abstract Command create();
	
	
	
	
	static{
		try{
			ServiceLoader<CommandFactory> loader = ServiceLoader.load(CommandFactory.class);
			for(CommandFactory cf:loader){
				cfs[cf.getOrderCode() & 0xFF] = cf;
			}
		}catch(Throwable thr){}
		
	}
}
