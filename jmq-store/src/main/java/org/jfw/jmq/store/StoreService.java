package org.jfw.jmq.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.Charset;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.zip.Adler32;

import org.jfw.jmq.log.LogFactory;
import org.jfw.jmq.log.Logger;
import org.jfw.jmq.store.config.StoreConfig;
import org.jfw.jmq.store.util.BufferFactory;
import org.jfw.jmq.store.util.IOHelper;
import org.jfw.jmq.store.util.InputFileSegment;
import org.jfw.jmq.store.util.LockFile;
import org.jfw.jmq.store.util.OutputFileSegment;

import com.google.gson.Gson;

public class StoreService extends LockFile {
	public static final Set<OpenOption> FILE_OPEN_OPTIONS;
	// @SuppressWarnings({"unchecked", "rawtypes"}) // generic array
	// construction
	public static final FileAttribute<?>[] FILE_NO_ATTRIBUTES = new FileAttribute[0];
	
	public static final Adler32 checkPointChecksum = new Adler32();
	public static final Adler32 redoChecksum = new Adler32();
	
	public static final Gson gson = new Gson();
	public static final Charset UTF8 = Charset.forName("UTF-8");
	
	synchronized public void start(StoreConfig config, File base, ExecutorService executor, BufferFactory bf)
			throws Exception {
		this.config = config;
		this.executor = executor;
		this.base = base;
		this.bf = bf;
		IOHelper.mkdirs(base);
		this.lock();
	

		this.init();
	

	}

	synchronized public void stop() {

		this.unlock();
	}

	protected void init() throws Exception {
		this.redoChannel = AsynchronousFileChannel.open(new File(this.base, "mq.redo").toPath(), FILE_OPEN_OPTIONS,
				this.executor, FILE_NO_ATTRIBUTES);
		this.cp1 = AsynchronousFileChannel.open(new File(this.base, "mq.cp1").toPath(), FILE_OPEN_OPTIONS,
				this.executor, FILE_NO_ATTRIBUTES);
		this.cp2 = AsynchronousFileChannel.open(new File(this.base, "mq.cp2").toPath(), FILE_OPEN_OPTIONS,
				this.executor, FILE_NO_ATTRIBUTES);
		this.redoChannel = AsynchronousFileChannel.open(new File(this.base, "mq.redo").toPath(), FILE_OPEN_OPTIONS,
				this.executor, FILE_NO_ATTRIBUTES);
		
		this.readMeta();
		
		this.apply(this.currentMeta);		
		
		if(!this.currentMeta.isClean()){
			this.recover();
		}
	}
	
	private void apply(StoreMeta meta){
		
	}
	
	protected void recover(){
		
		
		
	}
	public void store(boolean clean) throws Exception{
		this.output.reset(bf);
		try{
			this.currentMeta.incTime();
			this.currentMeta.setClean(clean);
			gson.toJson(this.currentMeta,new OutputStreamWriter(output, UTF8));
			output.close();
			List<ByteBuffer> buffers = output.getBuffers(checkPointChecksum);
			AsynchronousFileChannel ch = ((this.currentMeta.getTime() & 1) == 0)?cp2:cp1;
			store(ch,buffers);			
		}catch(Exception e){
			this.currentMeta.decTime();
			throw e;
		}
		finally{
			output.reset(bf);
		}
	}
	private void store(AsynchronousFileChannel ch,List<ByteBuffer> buffers) throws InterruptedException, ExecutionException, IOException{
		int idx   = 0;
		long pos = 0;
		while(idx<buffers.size()){
			ByteBuffer buf = buffers.get(idx);
			if(buf.hasRemaining()){
				int ret = ch.write(buf, pos).get();
				pos+=ret;
			}else{
				++idx;
			}
		}
		ch.force(false);
	}

	protected boolean readMeta() throws InterruptedException, ExecutionException{
		boolean ret = true;
		this.currentMeta = this.read(this.cp1);
		this.backupMeta = this.read(this.cp2);		
		if(this.currentMeta.getTime() < this.backupMeta.getTime()){
			StoreMeta meta = this.currentMeta;
			this.currentMeta = this.backupMeta;
			this.backupMeta = meta;
			ret = false;
		}
		return ret;
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

	// private boolean started =false;

	private StoreConfig config;
	private BufferFactory bf;
	private ExecutorService executor;
	private File base;
	private AsynchronousFileChannel redoChannel;
	

	private AsynchronousFileChannel cp1;
	private AsynchronousFileChannel cp2;
	
	private StoreMeta currentMeta ;
	private StoreMeta backupMeta;
	
	private InputFileSegment input = new InputFileSegment();
	private OutputFileSegment output = new OutputFileSegment();

	public File getBase() {
		return base;
	}

	
	
	
	
	private static final Logger log = LogFactory.getLog(StoreService.class);
	
	
	static {
		FILE_OPEN_OPTIONS = new HashSet<OpenOption>(3);
		FILE_OPEN_OPTIONS.add(StandardOpenOption.CREATE);
		FILE_OPEN_OPTIONS.add(StandardOpenOption.READ);
		FILE_OPEN_OPTIONS.add(StandardOpenOption.WRITE);

	}
}
