package org.jfw.jmq.store.command.written;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.AsynchronousSocketChannel;

import org.jfw.jmq.store.StoreClient;
import org.jfw.jmq.store.Topic;
import org.jfw.jmq.store.command.Command;

public class WriteCommand implements Command{
	private volatile long type;
	private volatile short size;
	private volatile ByteBuffer buf;
	private volatile AsynchronousSocketChannel reader;
	private volatile AsynchronousFileChannel writer;
	private volatile Topic topic;

	
	
	
	
	public AsynchronousSocketChannel getReader() {
		return reader;
	}

	public void setReader(AsynchronousSocketChannel reader) {
		this.reader = reader;
	}

	public AsynchronousFileChannel getWriter() {
		return writer;
	}

	public void setWriter(AsynchronousFileChannel writer) {
		this.writer = writer;
	}

	public ByteBuffer getBuf(){
		return buf;
	}
	

	public long getType() {
		return type;
	}

	public void setType(long type) {
		this.type = type;
	}

	public short getSize() {
		return size;
	}

	public void setSize(short size) {
		this.size = size;
	}

	
	public boolean isChunked(){
		return this.type < 0;
	}
	@Override
	public void exec(StoreClient client) {
		this.readType();		
	}
	
	
	public void failWithReadType(Throwable exc){}
	public void failWithReadSize(Throwable exc){}
	public void readType(){
		this.buf.position(0);
		this.buf.limit(8);
		this.reader.read(buf,this,ReadTypeHandler.INS);
	}
	public void readSize(){
		this.buf.position(0);
		this.buf.limit(2);
		this.reader.read(buf,this,ReadSizeHandler.INS);
	}
	public void read(){}

}
