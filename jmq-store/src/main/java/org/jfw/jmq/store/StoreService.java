package org.jfw.jmq.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;

import org.jfw.jmq.store.util.IOHelper;
import org.jfw.jmq.store.util.LockFile;

public class StoreService extends LockFile {

	synchronized public void start(File base, ExecutorService executor) throws Exception {
		this.executor = executor;
		this.base = base;
		IOHelper.mkdirs(base);
		this.lock();
		this.readMeta();

		this.init();

	}

	synchronized public void stop() {

		this.unlock();
	}

	protected void init() throws Exception {
		this.redoChannel = AsynchronousFileChannel.open(new File(this.base, "mq.redo").toPath(),
				StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
		this.cp1 = AsynchronousFileChannel.open(new File(this.base, "mq.cp1").toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
				StandardOpenOption.WRITE);
		this.cp2 = AsynchronousFileChannel.open(new File(this.base, "mq.cp2").toPath(), StandardOpenOption.CREATE,
				StandardOpenOption.READ, StandardOpenOption.WRITE);

	}

	protected void readMeta() throws Exception {
	

	}

	private StoreMeta read(File file) {
		if (!file.exists())
			return new StoreMeta();
		try {
			InputStream in = new FileInputStream(file);
			try {
				redoChannel.read(dst, position)
				
			} finally {
				IOHelper.close(in);
			}

		} catch (Exception e) {
			System.err.println("read file error:" + file.getAbsolutePath());
			e.printStackTrace();
			return new StoreMeta();
		}
	}

	// private boolean started =false;

	private ExecutorService executor;
	private File base;
	private AsynchronousFileChannel redoChannel;
	private AsynchronousFileChannel checkPointChannel;

	private AsynchronousFileChannel cp1;
	private AsynchronousFileChannel cp2;


	public File getBase() {
		return base;
	}

}
