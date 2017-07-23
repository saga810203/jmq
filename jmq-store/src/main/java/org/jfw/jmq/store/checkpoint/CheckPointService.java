package org.jfw.jmq.store.checkpoint;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.zip.Adler32;

import org.jfw.jmq.log.LogFactory;
import org.jfw.jmq.log.Logger;
import org.jfw.jmq.store.StoreMeta;
import org.jfw.jmq.store.StoreService;
import org.jfw.jmq.store.command.Command;
import org.jfw.jmq.store.redo.RedoService;
import org.jfw.jmq.store.util.BufferFactory;
import org.jfw.jmq.store.util.InputFileSegment;
import org.jfw.jmq.store.util.OutputFileSegment;

import com.google.gson.Gson;

public class CheckPointService implements Runnable{
	
	private static final Logger log = LogFactory.getLog(CheckPointService.class);
	
	public static final Adler32 checkPointChecksum = new Adler32();
	public static final Gson gson = new Gson();
	public static final Charset UTF8 = Charset.forName("UTF-8");
	

	private RedoService rds;
	private StoreService ss;
	
	private AsynchronousFileChannel cp1;
	private AsynchronousFileChannel cp2;
	private StoreMeta meta;
	private InputFileSegment input = new InputFileSegment();
	private OutputFileSegment output = new OutputFileSegment();
	private BufferFactory bf;
	private int redolimit;
	private int redoSize;
	private int maxRedoSize  ;
	
	private boolean running = false;
	private CountDownLatch stopLock;// = new CountDownLatch(1);
	

	
	
	public void init(StoreService ss,File base, ExecutorService executor,BufferFactory bf,RedoService rds,int maxRedoSize) throws IOException{
		this.ss =ss;
		this.bf = bf;
		this.rds = rds;
		this.maxRedoSize = maxRedoSize;
		this.cp1 = AsynchronousFileChannel.open(new File(base, "mq.cp1").toPath(), StoreService.FILE_OPEN_OPTIONS,
				executor, StoreService.FILE_NO_ATTRIBUTES);
		this.cp2 = AsynchronousFileChannel.open(new File(base, "mq.cp2").toPath(), StoreService.FILE_OPEN_OPTIONS,
				executor, StoreService.FILE_NO_ATTRIBUTES);
	}
	

	
	public boolean readStoreMeta() throws InterruptedException, ExecutionException{
		this.meta = this.read(this.cp1);
		StoreMeta meta2 = this.read(this.cp2);		
		if(meta.getTime() < meta2.getTime()){
			 meta =meta2;
		}
		return meta.isClean();
	}
	
	public StoreMeta getMeta(){
		return meta;
	}
	
	public void apply(StoreService storeService){
		
	}
	
	
	public void checkpoint(CheckPointSegment cps){
		for(Command cmd:cps.getCmds()){
			cmd.store(ss);
		}
		for(Command cmd:cps.getCmds()){
			cmd.apply(meta);
		}
		this.meta.setPosition(cps.getLimit());
		this.meta.setRedoTime(cps.getTime());
		this.redolimit = cps.getPosition();
		this.redoSize+=cps.getSize();
	}
	
	public void reStore() throws Exception{
		CheckPointSegment cps = null;
		while((cps = rds.pollCheckPointSegment())!=null){
			checkpoint(cps);
		}
		this.store(true);
		this.rds.setLimit(this.redolimit);
	}
	
	
	@Override
	public void run(){
		this.running = true;
		stopLock  = new CountDownLatch(1);
		this.redoSize = 0;
		CheckPointSegment cps = null;
		while(running){
			try {
				cps = rds.takeCheckPointSegment();
			} catch (InterruptedException e) {
				break;
			}
			this.checkpoint(cps);
			if(this.redoSize > this.maxRedoSize){
				try {
					this.store(false);
				} catch (Exception e) {
					log.error("checkpoint error", e);
					break;
				}
				this.redoSize =0;
				this.rds.setLimit(this.redolimit);
			}
		}
	}
	public void start(){
		
		
	}
	public void stop(){}
	
	
	
	
	
	private void store(boolean clean) throws Exception{
		this.output.reset(bf);
		try{
			this.meta.incTime();
			this.meta.setClean(clean);
			gson.toJson(this.meta,new OutputStreamWriter(output, UTF8));
			AsynchronousFileChannel ch = ((this.meta.getTime() & 1) == 0)?cp2:cp1;
			this.output.flushToFile(ch, checkPointChecksum,0,false);	
		}catch(Exception e){
			this.meta.decTime();
			throw e;
		}
		finally{
			output.reset(bf);
		}
	}
	
	
	
	private StoreMeta read(AsynchronousFileChannel channel) throws InterruptedException, ExecutionException {
		this.input.reset(bf, channel,0);
		int ret =this.input.load(checkPointChecksum);
		StoreMeta meta = null;
		if(ret>0){
			try{
				meta = gson.fromJson(new InputStreamReader(input, UTF8),StoreMeta.class);
			}catch(Exception e){
				log.warn("read checkpoint error,reason:parse error", e);
			}finally{
				input.freeBuffer();
			}
		}else{
			log.warn("read checkpoint error,reason:read bytes");
		}
		if(null == meta){
			meta = new StoreMeta();
		}
		return meta;
	}
	
	


}
