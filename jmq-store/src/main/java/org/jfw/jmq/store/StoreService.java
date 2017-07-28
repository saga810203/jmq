package org.jfw.jmq.store;

import java.io.File;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.jfw.jmq.log.LogFactory;
import org.jfw.jmq.log.Logger;
import org.jfw.jmq.store.checkpoint.CheckPointService;
import org.jfw.jmq.store.config.StoreConfig;
import org.jfw.jmq.store.redo.RedoService;
import org.jfw.jmq.store.util.BufferFactory;
import org.jfw.jmq.store.util.IOHelper;
import org.jfw.jmq.store.util.LockFile;

public class StoreService extends LockFile {
	public static final Set<OpenOption> FILE_OPEN_OPTIONS;
	// @SuppressWarnings({"unchecked", "rawtypes"}) // generic array
	// construction
	public static final FileAttribute<?>[] FILE_NO_ATTRIBUTES = new FileAttribute[0];

	
	private volatile CountDownLatch stopLock = null;

	synchronized public void start(StoreConfig config, File base, ExecutorService executor, BufferFactory bf) throws Exception {
		this.config = config;
		this.executor = executor;
		this.base = base;
		this.bf = bf;
		try {
			IOHelper.mkdirs(base);
			this.lock();
			this.init();
			this.rds.start();
			this.cps.start();
		} catch (Exception e) {
			this.rds.stop();
			this.cps.stop();
			this.unInit();
			this.unlock();
			throw e;
		}
		this.stopLock = new CountDownLatch(1);		
	}

	synchronized public void stop() {
		if(null!= this.stopLock){
			try {
				stopLock.await();
			} catch (InterruptedException e) {
			}
			this.stopLock = null;
		}
		this.unInit();
		this.unlock();
	}

	protected void init() throws Exception {
		try {
			this.rds.init(this, base, executor, 64 * 1024 * 4024);
			this.cps.init(this, base, executor, bf, rds, 32 * 1024 * 1024);
			if (!this.cps.readStoreMeta()) {
				this.cps.apply(this);
				this.rds.recover(this.cps.getMeta().getPosition(), this.cps.getMeta().getRedoTime());
				this.cps.reStore();
			} else {
				this.cps.apply(this);
			}
		} catch (Exception e) {
			this.cps.unInit();
			this.rds.unInit();
			throw e;
		}
	}

	protected void unInit() {
		this.cps.unInit();
		this.rds.unInit();
	}

	// private boolean started =false;

	private StoreConfig config;
	private BufferFactory bf;
	private ExecutorService executor;
	private File base;

	private CheckPointService cps = new CheckPointService();
	private RedoService rds = new RedoService();

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
