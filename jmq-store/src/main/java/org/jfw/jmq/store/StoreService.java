package org.jfw.jmq.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.zip.Adler32;

import org.jfw.jmq.store.config.StoreConfig;
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

	synchronized public void start(StoreConfig config, File base, ExecutorService executor, BufferFactory bf)
			throws Exception {
		this.config = config;
		this.executor = executor;
		this.base = base;
		this.bf = bf;
		IOHelper.mkdirs(base);
		this.lock();
		this.readMeta();

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
	}

	protected void readMeta() throws Exception {

	}

	private StoreMeta read(AsynchronousFileChannel channel) throws Exception {
		StoreMeta ret = null;
		ByteBuffer buf = this.bf.getBuffer();
		try {
			int size = IOHelper.readIntegerSize(channel, 0, buf);
			if (size == -1) {
				ret = new StoreMeta();
				ret.setCapacity(this.config.getRedoSize());
				ret.setPosition(0);
				ret.setTime(0);
			} else {

				byte[] bs = IOHelper.readByteContent(channel, 4, size, buf);
				

			}

		} finally {
			this.bf.freeBuffer(buf);
		}
		return ret;
	}

	// private boolean started =false;

	private StoreConfig config;
	private BufferFactory bf;
	private ExecutorService executor;
	private File base;
	private AsynchronousFileChannel redoChannel;
	private AsynchronousFileChannel checkPointChannel;

	private AsynchronousFileChannel cp1;
	private AsynchronousFileChannel cp2;

	public File getBase() {
		return base;
	}

	static {
		FILE_OPEN_OPTIONS = new HashSet<OpenOption>(3);
		FILE_OPEN_OPTIONS.add(StandardOpenOption.CREATE);
		FILE_OPEN_OPTIONS.add(StandardOpenOption.READ);
		FILE_OPEN_OPTIONS.add(StandardOpenOption.WRITE);

	}
}
