package org.jfw.jmq.store.redo;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.Adler32;

import org.jfw.jmq.store.StoreService;
import org.jfw.jmq.store.checkpoint.CheckPointSegment;
import org.jfw.jmq.store.command.Command;
import org.jfw.jmq.store.command.CommandFactory;
import org.jfw.jmq.store.exception.StoreException;

public class RedoService implements Runnable {
	public static final Adler32 adler = new Adler32();

	private boolean running;

	private StoreService ss = null;
	private FileChannel channel;
	private MappedByteBuffer buffer;
	private ExecutorService executor;
	private LinkedBlockingQueue<RedoSegment> queue = new LinkedBlockingQueue<RedoSegment>();
	private LinkedBlockingQueue<CheckPointSegment> cqueue = new LinkedBlockingQueue<CheckPointSegment>();

	private RedoSegment crs;

	private int capacity;
	private volatile int limit;
	private int position;

	private int nextPosition;
	private int segmentSize;

	private long time;

	private CountDownLatch stopLock;

	private ArrayList<Command> cmds;
	private Thread thread;

	public synchronized void init(StoreService ss, File base, ExecutorService executor, int capacity) throws IOException {
		if (this.ss == null) {
			this.channel = FileChannel.open(new File(base, "mq.redo").toPath(), StoreService.FILE_OPEN_OPTIONS, StoreService.FILE_NO_ATTRIBUTES);
			this.buffer = this.channel.map(MapMode.READ_WRITE, 0, capacity);
			this.running = false;
			this.ss = ss;
			this.executor = executor;
			this.capacity = capacity;
			// this.buffer.load();
		}
	}

	public synchronized void unInit() {
		if (ss == null)
			return;
		if (running)
			return;
		if (null != thread)
			return;
		try {
			this.channel.close();
		} catch (IOException e) {
		}
		this.ss = null;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	private void takeRedoSegment() throws InterruptedException {
		if (null == this.crs) {
			this.crs = this.queue.take();
			this.cmds = crs.getCmds();
			this.segmentSize = 24;// 4+8+4+8; // segLength, checksum,segPos,time
			for (Command cmd : this.cmds) {
				this.segmentSize = +cmd.getRedoSize();
			}
			nextPosition = this.position + this.segmentSize;
		}
	}

	private boolean hasSpace() {
		int climit = this.limit;

		if (this.nextPosition > this.capacity) {
			if (this.position <= climit) {
				return false;
			}
			if (climit < this.segmentSize) {
				return false;
			}
			int nss = this.capacity - this.position + this.segmentSize;
			this.position = 0;
			this.nextPosition = this.segmentSize;
			this.segmentSize = nss;
			return true;
		}
		return climit >= this.nextPosition;
	}

	private void flush() throws Exception {
		if (this.position == 0) {
			this.buffer.clear();
		}
		this.buffer.putInt(nextPosition - position - 4);
		this.buffer.putLong(0);
		this.buffer.putInt(this.position);
		this.buffer.putLong(this.time);
		for (Command cmd : this.cmds) {
			cmd.writeRedo(this.buffer);
		}
		this.buffer.flip();
		this.buffer.position(this.position + 12);
		adler.reset();
		adler.update(this.buffer);
		this.buffer.position(this.position + 4);
		this.buffer.putLong(adler.getValue());
		this.buffer.position(this.nextPosition);
		this.buffer.force();
	}

	private void beforeHand() {
		this.buffer.clear();
		this.limit = this.capacity - 1;
		this.cmds.clear();
		this.queue.clear();
		this.cqueue.clear();
		this.stopLock = new CountDownLatch(1);
		this.running = true;
		this.time = 1;
	}

	private void hand() {
		while (running) {
			try {
				this.takeRedoSegment();
			} catch (InterruptedException e1) {
				if (!running) {
					return;
				}
			}
			if (this.hasSpace()) {
				try {
					this.flush();
				} catch (Exception e) {
					this.crs.fail(new StoreException(StoreException.WRITE_RODO_ERROR, e), executor);
					this.crs = null;
					break;
				}
				for (Command cmd : this.cmds) {
					cmd.afterCommit(ss);
				}
				++this.time;
				this.cqueue.add(new CheckPointSegment(cmds, position, nextPosition, segmentSize, time));
				this.position = nextPosition;
				this.crs = null;
			}
		}
	}

	@Override
	public void run() {
		this.beforeHand();
		this.hand();
		this.clean();
	}

	private static final StoreException E_STORE_IS_SHUTDOWN = new StoreException(StoreException.STORE_IS_SHUTDOWN, "store is shutdown");

	private void clean() {
		// TODO clean resource

		if (this.crs != null) {
			crs.fail(E_STORE_IS_SHUTDOWN, executor);
		}
		while ((this.crs = this.queue.poll()) != null) {
			crs.fail(E_STORE_IS_SHUTDOWN, executor);
		}
		this.stopLock.countDown();
	}

	public void recover(int pos, long time) throws StoreException {
		try {
			int ret = 0;
			while ((ret = this.readSegment(pos, time)) > 0) {
				pos = ret;
				++time;
			}
			pos = 0;
			while ((ret = this.readSegment(pos, time)) > 0) {
				pos = ret;
				++time;
			}
		} catch (Throwable thr) {
			throw new StoreException(StoreException.READ_RODO_ERROR, thr);
		}
	}

	private int readSegment(int pos, long ntime) {
		int dpos = pos + 24;
		if (dpos >= this.capacity) {
			return -1;
		}
		this.buffer.position(pos);
		int len = this.buffer.getInt();
		if (len <= 20) {
			return -1;
		}
		int npos = pos + 4 + len;
		if (npos > this.capacity) {
			return -1;
		}
		long checksum = this.buffer.getLong();

		if (pos != this.buffer.getInt()) {
			return -1;
		}
		if (ntime != this.buffer.getLong()) {
			return -1;
		}
		this.buffer.position(pos + 12);
		this.buffer.limit(npos);
		adler.reset();
		adler.update(this.buffer);
		if (checksum != adler.getValue()) {
			return -1;
		}
		ArrayList<Command> rcmds = new ArrayList<Command>();
		for (;;) {
			int ret = readCmd(dpos, rcmds);
			if (ret > dpos && ret <= npos) {
				if (ret == npos) {
					break;
				} else {
					dpos = ret;
				}
			} else {
				return -1;
			}
		}
		++ntime;
		this.cqueue.add(new CheckPointSegment(rcmds, pos, npos, 1, ntime));
		return npos;
	}

	private int readCmd(int pos, ArrayList<Command> ret) {
		this.buffer.clear();
		this.buffer.position(pos);
		byte c = this.buffer.get();
		this.buffer.position(pos);
		CommandFactory cf = CommandFactory.get(c);
		if (cf != null) {
			Command cmd = cf.create();
			if (cmd.readRedo(this.buffer)) {
				ret.add(cmd);
				return pos + cmd.getRedoSize();
			}
			return -2;
		}
		return -1;
	}

	public CheckPointSegment pollCheckPointSegment() {
		return this.cqueue.poll();
	}

	public CheckPointSegment takeCheckPointSegment() throws InterruptedException {
		return this.cqueue.take();
	}

	public void addRedoSegment(RedoSegment redosegment) {
		this.queue.add(redosegment);
	}

	public synchronized void start() {
		if (this.ss == null)
			return;
		if (null != thread)
			return;
		thread = new Thread(this);
		thread.start();
	}

	public synchronized void stop() {
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
}
