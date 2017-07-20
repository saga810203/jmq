package org.jfw.jmq.store;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.zip.Adler32;

import org.jfw.jmq.log.LogFactory;
import org.jfw.jmq.log.Logger;
import org.jfw.jmq.store.redo.RedoService;
import org.jfw.jmq.store.util.BufferFactory;
import org.jfw.jmq.store.util.InputFileSegment;
import org.jfw.jmq.store.util.OutputFileSegment;

import com.google.gson.Gson;

public class CheckPointService{
	private static final Logger log = LogFactory.getLog(CheckPointService.class);
	
	public static final Adler32 checkPointChecksum = new Adler32();
	public static final Gson gson = new Gson();
	public static final Charset UTF8 = Charset.forName("UTF-8");
	

	private RedoService rds;
	
	private AsynchronousFileChannel cp1;
	private AsynchronousFileChannel cp2;
	private StoreMeta meta;
	private InputFileSegment input = new InputFileSegment();
	private OutputFileSegment output = new OutputFileSegment();
	private BufferFactory bf;
	
	
	public void init(File base, ExecutorService executor,BufferFactory bf,RedoService rds) throws IOException{
		this.bf = bf;
		this.rds = rds;
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
	
	public void checkPoint() throws Exception{
		
		
		this.store(true);
	}
	
	public void start(){}
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
