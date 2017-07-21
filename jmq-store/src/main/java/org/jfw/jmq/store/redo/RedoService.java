package org.jfw.jmq.store.redo;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.zip.Adler32;

import org.jfw.jmq.store.StoreService;
import org.jfw.jmq.store.checkpoint.CheckPointSegment;
import org.jfw.jmq.store.command.Command;
import org.jfw.jmq.store.command.CommandFactory;
import org.jfw.jmq.store.exception.StoreException;

public class RedoService implements Runnable {
	public static final Adler32 adler = new Adler32();

	private boolean running;

	private StoreService ss;
	private FileChannel channel;
	private MappedByteBuffer buffer;
	private ExecutorService executor;
	private Queue<RedoSegment> queue = new ConcurrentLinkedQueue<RedoSegment>();
	private Queue<CheckPointSegment> cqueue = new ConcurrentLinkedQueue<CheckPointSegment>();

	private RedoSegment crs;
	private boolean reClac;

	private int capacity;
	private volatile int limit;
	private int position;

	private int nextPosition;

	private long time;
	
	
	private CountDownLatch stopLock;

	private ArrayList<Command> cmds;

	public void init(StoreService ss, File base, ExecutorService executor, int capacity) throws IOException {
		this.ss = ss;
		this.executor = executor;
		this.capacity = capacity;
		this.channel = FileChannel.open(new File(base, "mq.redo").toPath(), StoreService.FILE_OPEN_OPTIONS, StoreService.FILE_NO_ATTRIBUTES);
		this.buffer = this.channel.map(MapMode.READ_WRITE, 0, this.capacity);
		// this.buffer.load();
	}


	
	private void clacPosition(){
		this.cmds = crs.getCmds();
		int  segLen=24;// 4+8+4+8;  // segLength, checksum,segPos,time
		for(Command cmd :this.cmds){
			segLen=+cmd.getRedoSize();
		}
		nextPosition = this.position+segLen;
		if(nextPosition >this.capacity){
			position = 0;
			nextPosition = segLen;
		}
	}
	private boolean hasSpace(){
		int climit = this.limit;
		return (climit >= nextPosition) || ( nextPosition > climit && position > climit);
	}
	
	private void flush() throws Exception{
		if(this.position==0){
			this.buffer.clear();
		}
		this.buffer.putInt(nextPosition-position-4);
		this.buffer.putLong(0);
		this.buffer.putInt(this.position);
		this.buffer.putLong(this.time);
		for(Command cmd:this.cmds){
			cmd.writeRedo(this.buffer);
		}
		this.buffer.flip();
		this.buffer.position(this.position+12);
		adler.reset();
		adler.update(this.buffer);
		this.buffer.position(this.position+4);
		this.buffer.putLong(adler.getValue());
		this.buffer.position(this.nextPosition);
		this.buffer.force();		
	}
	

	@Override
	public void run() {
		this.buffer.clear();
		this.limit = this.capacity-1;
		this.cmds.clear();
		this.queue.clear();
		this.cqueue.clear();
		this.stopLock = new CountDownLatch(1);
		this.running = true;
		
		reClac = true;
		while (running) {
			if (this.crs == null) {
				this.crs = this.queue.poll();
				reClac = true;
			}
			if (this.crs != null) {
				if(reClac){
					clacPosition();
					reClac = false;
				}
				if(this.hasSpace()){
					try {
						this.flush();
					} catch (Exception e) {
						this.crs.fail(new StoreException(StoreException.WRITE_RODO_ERROR, e), executor);
						this.crs = null;
						break;
					}
					for(Command cmd:this.cmds){
						cmd.afterCommit(ss);
					}
					this.cqueue.add(new CheckPointSegment(cmds, position,nextPosition,time));
					++this.time;
					this.position = nextPosition;
					this.crs = this.queue.poll();
					this.reClac = true;
				}
			} else {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}
		}
		this.clean();
	}
	
	private static final StoreException  E_STORE_IS_SHUTDOWN = new StoreException(StoreException.STORE_IS_SHUTDOWN, "store is shutdown");
	private void clean(){
		//TODO clean resource
		
		if(this.crs!=null){
			crs.fail(E_STORE_IS_SHUTDOWN, executor);
		}
		while((this.crs=this.queue.poll())!=null){
			crs.fail(E_STORE_IS_SHUTDOWN, executor);
		}
		try {
			this.channel.close();
		} catch (IOException e) {
		}
		this.stopLock.countDown();
	}
	
	public void recover(int pos,long time) throws StoreException {
		try{
		int ret = 0;
		while((ret =this.readSegment(pos, time))>0){
			pos = ret ;
			++time;
		}
	    pos = 0;
		while((ret =this.readSegment(pos, time))>0){
			pos = ret ;
			++time;
		} 
		}catch(Throwable thr){
			throw new StoreException(StoreException.READ_RODO_ERROR,thr);
		}
	}
	
	private int readSegment(int pos,long time){
		int dpos = pos+24;
		if(dpos>=this.capacity){
			return -1;
		}
		this.buffer.position(pos);
		int len = this.buffer.getInt();
		if(len<=20){
			return -1;
		}
		int npos = pos+4+len;
		if(npos>this.capacity){
			return -1;
		}
		long checksum = this.buffer.getLong();
		
		if(pos!= this.buffer.getInt()){
			return -1;
		}
		if(time != this.buffer.getLong()){
			return -1; 
		}
		this.buffer.position(pos+12);
		this.buffer.limit(npos);
		adler.reset();
		adler.update(this.buffer);
		if(checksum!=adler.getValue()){
			return -1;
		}
		ArrayList<Command> rcmds = new ArrayList<Command>();
		for(;;){
			int ret = readCmd(dpos, rcmds);
			if(ret > dpos && ret <= npos){
				if(ret == npos){
					break;
				}else{
					dpos = ret;
				}
			}else{
				return -1;
			}
		}
		this.cqueue.add(new CheckPointSegment(rcmds, pos,npos, time));
		return npos;
		
	}
	
	private int readCmd(int pos,ArrayList<Command> ret){
		this.buffer.clear();
		this.buffer.position(pos);
		byte c = this.buffer.get();
		this.buffer.position(pos);
		CommandFactory cf = CommandFactory.get(c);
		if(cf != null){
			Command cmd = cf.create();
			if(cmd.readRedo(this.buffer)){
				ret.add(cmd);
				return pos + cmd.getRedoSize();
			}
			return -2;
		}
		return -1;
	}
	

	public void start() {
	}

	public void stop() {
		this.running = false;
		try {
			this.stopLock.await();
		} catch (InterruptedException e) {
		}
	}


}
