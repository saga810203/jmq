package org.jfw.jmq.store.command.written;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.AsynchronousSocketChannel;

import org.jfw.jmq.store.StoreClient;
import org.jfw.jmq.store.Topic;
import org.jfw.jmq.store.command.Command;

public class WriteCommand implements Command {
	public static final short MAX_CHUNK_DATA_LENGTH = 8192 - 2 - 8;
	private long type;
	private short size;
	private short rsize;
	private long position = 0;
	private long writoePos = -1;
	private boolean chunked = false;
	private long startIdx;
	private long end;
	// private volatile short limit;
	private ByteBuffer buf;
	private AsynchronousSocketChannel reader;
	private AsynchronousFileChannel writer;
	private Topic topic;
	private StoreClient client;

	public long getWritoePos() {
		return writoePos;
	}

	public void setWritoePos(long writoePos) {
		this.writoePos = writoePos;
	}

	public void incWritoePos(int v) {
		this.writoePos += v;
	}

	public long getPosition() {
		return position;
	}

	public void setPosition(long position) {
		this.position = position;
	}

	public Topic getTopic() {
		return topic;
	}

	public void setTopic(Topic topic) {
		this.topic = topic;
	}

	public AsynchronousSocketChannel getReader() {
		return reader;
	}

	public void setReader(AsynchronousSocketChannel reader) {
		this.reader = reader;
	}

	public AsynchronousFileChannel getWriter() {
		return writer;
	}

	public void setWriter(AsynchronousFileChannel writer) {
		this.writer = writer;
	}

	public ByteBuffer getBuf() {
		return buf;
	}

	public long getType() {
		return type;
	}

	public void setType(long type) {
		this.type = type;
	}

	public short getSize() {
		return size;
	}

	public void setSize(short size) {
		this.size = size;
		if (this.size > 0) {
			this.chunked = false;
			rsize = size;
		} else {
			this.chunked = true;
			rsize = (short) -size;
		}

	}

	public boolean isChunked() {
		return this.chunked;
	}

	public boolean checkSize() {
		if (chunked) {
			if (rsize > MAX_CHUNK_DATA_LENGTH) {
				this.failWithInvalidSize();
				return false;
			}
		} else {
			if (size > 8190) {
				this.failWithInvalidSize();
				return false;
			}
		}
		return true;
	}

	public void reset(Topic topic) {
		this.topic = topic;
		this.position = 0;
		this.writoePos = -1;
		this.writer = topic.getChannel();
	}

	@Override
	public void exec(StoreClient client) {
		this.topic = client.getCurrentTopic();
		this.position = 0;
		this.writoePos = -1;
		this.writer = topic.getChannel();
		this.reader = client.getChannel();
		this.buf = client.allocateBuf();
		this.startIdx = -1;
		this.readType();
	}

	public void failWithRead(Throwable exc) {
		// TODO:impl
	}

	public void failWithWirte(Throwable exc) {
		// TODO:impl
	}

	public void failWithInvalidSize() {
		// TODO:impl
	}

	public void failWithAllocate() {

	}

	public void done(long end) {
		this.end = end;
		this.client.over(this);		
	}

	public void readType() {
		this.buf.clear();
		this.buf.limit(8);
		this.reader.read(buf, this, ReadTypeHandler.INS);
	}

	public void readSize() {
		this.position =-1;
		this.buf.clear();
		this.buf.limit(2);
		this.reader.read(buf, this, ReadSizeHandler.INS);
	}

	private void allocate() {
//		if (this.position == -1) {
//			this.position = this.topic.allocate(( this.chunked ? rsize + 8 : rsize)+2);
//			if (position == -1) {
//				this.failWithAllocate();
//			} else {
//				if (-1 == this.startIdx) {
//					this.startIdx = position;
//				}
//			}
//		}
	}

	public void readData() {
		this.allocate();
		if (-1 != this.position) {
			if (-1 == this.writoePos) {
				this.buf.clear();
				this.buf.putShort(this.size);
				this.buf.limit(2 + this.rsize);
				this.reader.read(buf, this, ReadDataHandler.INS);
			} else {
				this.writeNextPos();
			}
		}
	}

	public void writeData() {
		this.writoePos = this.position;
		this.buf.flip();
		this.writer.write(buf, this.writoePos, this, WriteDataHandler.INS);
	}

	public void writeNextPos() {
		this.buf.clear();
		this.buf.putLong(this.position);
		this.buf.flip();
		this.writer.write(buf, this.writoePos, this, WritePosHandler.INS);
	}

	@Override
	public void commit(StoreClient client) {
		// TODO Auto-generated method stub
		
	}
}
