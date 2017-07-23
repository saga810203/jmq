package org.jfw.jmq.store.redo;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;

import org.jfw.jmq.store.command.Command;

public abstract class RedoSegment implements Runnable{

	private ArrayList<Command> cmds = new ArrayList<Command>();
	private long position;
	private long limit;
	
	private Throwable thr;
	
	private Runnable runner;


	public ArrayList<Command> getCmds() {
		return cmds;
	}


	public void setCmds(ArrayList<Command> cmds) {
		this.cmds = cmds;
	}


	public long getPosition() {
		return position;
	}


	public void setPosition(long position) {
		this.position = position;
	}


	public long getLimit() {
		return limit;
	}


	public void setLimit(long limit) {
		this.limit = limit;
	}
	public Runnable getRunner() {
		return runner;
	}
	public void setRunner(Runnable runner) {
		this.runner = runner;
	}
	public void success(ExecutorService executor){
		this.thr = null;
        executor.submit(this);		
	}
	public void fail(Throwable thr,ExecutorService executor){
		this.thr = thr;
        executor.submit(this);
	}
	
	public boolean isSucces(){
		return null == thr;
	}

}
