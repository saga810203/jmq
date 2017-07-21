package org.jfw.jmq.store.checkpoint;

import java.util.ArrayList;

import org.jfw.jmq.store.command.Command;

public class CheckPointSegment {
	private int position;
	private long time;
	private int limit;
	private ArrayList<Command> cmds;
	
	
	
	
	public CheckPointSegment(ArrayList<Command> cmds ,int position,int limit,long time){
		this.cmds = (ArrayList<Command>) cmds.clone();
		this.position = position;
		this.time = time;
		this.limit = limit;
	}

}
