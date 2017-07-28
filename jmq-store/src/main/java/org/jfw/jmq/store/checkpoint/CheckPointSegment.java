package org.jfw.jmq.store.checkpoint;

import java.util.ArrayList;

import org.jfw.jmq.store.command.Command;

public class CheckPointSegment {
	private int position;
	private long time;
	private int limit;
	private int size;
	private ArrayList<Command> cmds;

	@SuppressWarnings("unchecked")
	public CheckPointSegment(ArrayList<Command> cmds, int position, int limit,int size, long time) {
		this.cmds = ((ArrayList<Command>) cmds.clone());
		this.position = position;
		this.time = time;
		this.limit = limit;
		this.size = size;
	}

	public int getPosition() {
		return position;
	}

	public long getTime() {
		return time;
	}

	public int getLimit() {
		return limit;
	}

	public ArrayList<Command> getCmds() {
		return cmds;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}
}
