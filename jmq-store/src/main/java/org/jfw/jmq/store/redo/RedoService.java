package org.jfw.jmq.store.redo;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.zip.Adler32;

import org.jfw.jmq.store.StoreService;
import org.jfw.jmq.store.command.Command;
import org.jfw.jmq.store.util.BufferFactory;

public class RedoService extends OutputStream implements Runnable{
	public static final Adler32 adler = new Adler32();
	
	private boolean running;
	
	private StoreService ss;
	private FileChannel channel;
	private MappedByteBuffer buffer;
	private ExecutorService executor;
	private Queue<RedoSegment> queue = new ConcurrentLinkedQueue<RedoSegment>();
	
	private RedoSegment crs;



	private volatile long capacity;
	private volatile long limit;
	private long position;
	
	private long nextPosition;
	
	private long time;
	
	private List<Command> cmds;
	
	public void init(StoreService ss,File base, ExecutorService executor,int capacity) throws IOException{
		this.ss = ss;
		this.executor = executor;
		this.capacity = capacity;
		this.channel = FileChannel.open(new File(base, "mq.redo").toPath(),  StoreService.FILE_OPEN_OPTIONS,	StoreService.FILE_NO_ATTRIBUTES);
		this.buffer = this.channel.map(MapMode.READ_WRITE, 0,this.capacity);
		//this.buffer.load();
	}
	
	private void flushHead(){
		this.length = 16;
		this.bf.freeBuffer(this.buffers);
		this.head = this.bf.getBuffer();
		this.current = this.head;
		this.buffers.add(this.head);
		this.head.position(12);
		this.head.putLong(this.position);
		this.head.putLong(this.time);
	}
	private void flushCmds(){
		for(Command cmd:cmds){
			cmd.writeRedo(this);
		}
		this.current.flip();		
		this.newPosition = this.position+this.length+12;
		if(this.newPosition>= this.capacity){
			this.position =0;
			this.newPosition = this.length+12;
			this.head.position(12);
			this.head.putLong(0);
		}
		this.head.position(12);
		adler.reset();
		for(ByteBuffer b:this.buffers){
			adler.update(b);
			b.rewind();
		}
		this.head.putInt(this.length+8);
		this.head.putLong(adler.getValue());
		this.head.rewind();		
	}
	
	
	
	
	
	
	protected void flushAndForce( ){
		this.flushHead();
		this.flushCmds();
		long climit = this.limit;
		if((this.position > climit)||(this.position< climit && this.newPosition < climit )){
			
			
			
		}else{
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
			}
		}
		
		

	}
	
	
	
	
	private boolean takeNext(){
		if(this.crs == null){
			this.crs = this.queue.poll();
		}
		return null != this.crs;
	}


	
	
	
	
	
	
	

	@Override
	public void run() {
		while(running){
			
		}
		
	}

	public	 void recover(){
		
	}
	
	public void start(){}
	public void stop(){}

	
	
	
	
	
	
	
	
	
	
	
	
	
	@Override
	public void write(int b){
		if(this.current.hasRemaining()){
			this.current.put((byte)b);
		}else{
			this.current.flip();
			this.current = this.bf.getBuffer();
			this.buffers.add(this.current);
			this.current.put((byte)b);
		}
		++this.length;
	}

	@Override
	public void write(byte[] b) {
		this.write(b,0,b.length);
	}
	@Override
	public void write(byte[] b, int off, int len)  {
		while(len>0){
			int l = this.current.remaining();
			if(l>0){
				if(l>=len){
					this.current.put(b,off, len);
					this.length=+len;
				}else{
					this.current.put(b,off,l);
					off=+l;
					len-=l;
					this.length+=l;
				}
			}else{
				this.current.flip();
				this.current = this.bf.getBuffer();
				this.buffers.add(current);
			}
		}
	}
}
