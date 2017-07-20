package org.jfw.jmq.store.redo;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.zip.Adler32;

import org.jfw.jmq.store.command.Command;
import org.jfw.jmq.store.util.BufferFactory;
import org.jfw.jmq.store.util.OutputFileSegment;

public class RedoSegment{

	private ArrayList<Command> cmds = new ArrayList<Command>();
	private long position;
	private long limit;
	
	
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
}
