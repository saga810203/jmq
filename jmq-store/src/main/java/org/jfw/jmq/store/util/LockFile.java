package org.jfw.jmq.store.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;


public abstract class LockFile {
	public abstract File getBase();

	private File lockFile;
	private FileLock fileLock;
	private RandomAccessFile randomAccessLockFile;

	protected void lock() throws IOException {
		this.lockFile = new File(this.getBase(), "lock");
		this.randomAccessLockFile = new RandomAccessFile(lockFile, "rw");
		IOException reason = null;
		try {
			fileLock = randomAccessLockFile.getChannel().tryLock(0,
					Math.max(1, randomAccessLockFile.getChannel().size()), false);
		} catch (OverlappingFileLockException e) {
			reason = new IOException("File '" + lockFile + "' could not be locked.", e);
		} catch (IOException ioe) {
			reason = ioe;
		}
		if (fileLock != null) {
			randomAccessLockFile.writeLong(System.currentTimeMillis());
			randomAccessLockFile.getChannel().force(true);
		} else {
			closeLockFile();
			if (reason != null) {
				throw reason;
			}
			throw new IOException("File '" + lockFile + "' could not be locked.");
		}
	}
    private void closeLockFile() {
        // close the file.
        if (randomAccessLockFile != null) {
            try {
                randomAccessLockFile.close();
            } catch (Throwable ignore) {
            }
            randomAccessLockFile = null;
        }
    }
    
    synchronized protected void unlock() {
      
        // release the lock..
        if (fileLock != null) {
            try {
                fileLock.release();
            } catch (Throwable ignore) {
            } finally {
            	fileLock = null;
            }
        }
        closeLockFile();
        if(this.lockFile!=null){
        	this.lockFile.delete();
        }
    }

}
