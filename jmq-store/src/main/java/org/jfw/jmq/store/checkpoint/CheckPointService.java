package org.jfw.jmq.store.checkpoint;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.Charset;
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
import org.jfw.jmq.store.util.IOHelper;
import org.jfw.jmq.store.util.InputFileSegment;
import org.jfw.jmq.store.util.OutputFileSegment;

import com.google.gson.Gson;

public class CheckPointService implements Runnable {

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
	private int maxRedoSize;
	private long lastRedoTime = -1;

	private CheckPointSegment cps;

	private boolean running = false;
	private CountDownLatch stopLock;// = new CountDownLatch(1);

	private Thread thread = null;

	public synchronized void init(StoreService ss, File base, ExecutorService executor, BufferFactory bf, RedoService rds, int maxRedoSize) throws IOException {
		if (this.ss == null) {

			this.cp1 = AsynchronousFileChannel.open(new File(base, "mq.cp1").toPath(), StoreService.FILE_OPEN_OPTIONS, executor,
					StoreService.FILE_NO_ATTRIBUTES);
			try {
				this.cp2 = AsynchronousFileChannel.open(new File(base, "mq.cp2").toPath(), StoreService.FILE_OPEN_OPTIONS, executor,
						StoreService.FILE_NO_ATTRIBUTES);
			} catch (IOException e) {
				IOHelper.close(cp1);
				throw e;
			}
			this.ss = ss;
			this.bf = bf;
			this.rds = rds;
			this.maxRedoSize = maxRedoSize;
		}
	}

	public synchronized void unInit() {
		this.stop();
		if (this.ss != null) {
			IOHelper.close(cp1);
			IOHelper.close(cp2);
			this.ss = null;
			this.rds = null;
		}
	}

	public boolean readStoreMeta() throws InterruptedException, ExecutionException {
		this.meta = this.read(this.cp1);
		StoreMeta meta2 = this.read(this.cp2);
		if (meta.getTime() < meta2.getTime()) {
			meta = meta2;
		}
		return meta.isClean();
	}

	public StoreMeta getMeta() {
		return meta;
	}

	public void apply(StoreService storeService) {

	}

	public void doCheckpoint() {
		for (Command cmd : cps.getCmds()) {
			cmd.store(ss);
		}
		for (Command cmd : cps.getCmds()) {
			cmd.apply(meta);
		}
		this.meta.setPosition(cps.getLimit());
		this.meta.setRedoTime(cps.getTime());
		this.redolimit = cps.getPosition();
		this.redoSize += cps.getSize();
	}

	public void reStore() throws Exception {
		while ((cps = rds.pollCheckPointSegment()) != null) {
			doCheckpoint();
		}
		this.store(true);
		this.rds.setLimit(this.redolimit);
	}

	private void beforeHand() {
		this.running = true;
		stopLock = new CountDownLatch(1);
		this.redoSize = 0;
	}

	private void hand() {
		while (running) {
			try {
				cps = rds.takeCheckPointSegment();
			} catch (InterruptedException e) {
				continue;
			}
			long lrt = cps.getTime();
			if (this.lastRedoTime != lrt) {
				doCheckpoint();
				this.lastRedoTime = lrt;
				if (this.redoSize > this.maxRedoSize) {
					try {
						this.store(false);
					} catch (Exception e) {
						log.error("checkpoint error", e);
						break;
					}
					this.redoSize = 0;
					this.rds.setLimit(this.redolimit);
				}
			}
		}
	}

	private void clean() {
		if (null != this.cps && this.cps.getTime() != this.lastRedoTime) {
			doCheckpoint();
		}
		while ((cps = rds.pollCheckPointSegment()) != null) {
			doCheckpoint();
		}
		try {
			this.store(true);
		} catch (Exception e) {
			log.error("checkpoint error", e);
		}
		this.stopLock.countDown();
	}

	@Override
	public void run() {
		this.beforeHand();
		this.hand();
		this.clean();
	}

	public synchronized void start() {
		if (null == this.ss) {
			return;
		}
		if (null != thread) {
			return;
		}
		thread = new Thread(this);
		thread.start();
	}

	public void stop() {
		if (null != thread) {
			this.running = false;
			thread.interrupt();
			thread = null;
			try {
				this.stopLock.await();
			} catch (InterruptedException e) {
			}
		}
	}

	private void store(boolean clean) throws Exception {
		this.output.reset(bf);
		try {
			this.meta.incTime();
			this.meta.setClean(clean);
			gson.toJson(this.meta, new OutputStreamWriter(output, UTF8));
			AsynchronousFileChannel ch = ((this.meta.getTime() & 1) == 0) ? cp2 : cp1;
			this.output.flushToFile(ch, checkPointChecksum, 0, false);
		} catch (Exception e) {
			this.meta.decTime();
			throw e;
		} finally {
			output.reset(bf);
		}
	}

	private StoreMeta read(AsynchronousFileChannel channel) throws InterruptedException, ExecutionException {
		this.input.reset(bf, channel, 0);
		int ret = this.input.load(checkPointChecksum);
		StoreMeta meta = null;
		if (ret > 0) {
			try {
				meta = gson.fromJson(new InputStreamReader(input, UTF8), StoreMeta.class);
			} catch (Exception e) {
				log.warn("read checkpoint error,reason:parse error", e);
			} finally {
				input.freeBuffer();
			}
		} else {
			log.warn("read checkpoint error,reason:read bytes");
		}
		if (null == meta) {
			meta = new StoreMeta();
		}
		return meta;
	}

}
