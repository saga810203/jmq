package org.jfw.jmq.store;

import java.io.File;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.zip.Adler32;

import org.jfw.jmq.log.LogFactory;
import org.jfw.jmq.log.Logger;
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

	public static final Adler32 checkPointChecksum = new Adler32();
	public static final Adler32 redoChecksum = new Adler32();

	synchronized public void start(StoreConfig config, File base, ExecutorService executor, BufferFactory bf) throws Exception {
		this.config = config;
		this.executor = executor;
		this.base = base;
		this.bf = bf;
		IOHelper.mkdirs(base);
		this.lock();
		this.init();
		this.rds.start();
		this.cps.start();
	}

	synchronized public void stop() {

		this.unlock();
	}

	protected void init() throws Exception {
		this.rds.init(this, base, executor, 64 * 1024 * 4024);
		this.cps.init(base, executor, bf, rds);
		if (!this.cps.readStoreMeta()) {
			this.cps.apply(this);
			this.rds.recover(this.cps.getMeta().getPosition(), this.cps.getMeta().getRedoTime());
			this.cps.reStore();
		} else {
			this.cps.apply(this);
		}
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
