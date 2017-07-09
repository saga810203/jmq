package org.jfw.jmq.store.util;

import java.util.concurrent.locks.ReentrantLock;

public class LoopedFile {
	
    private long position = 0;
    private long limit;
    private long capacity;
    
    private ReentrantLock lock = new ReentrantLock();
    
    
    public LoopedFile(long capacity,long limit,long position){
    	this.capacity = capacity;
    	this.limit = limit;
    	this.position = position;
    }
    
    
//    private AsynchronousFileChannel channel; 
    

    public long allocate(long size){
    	lock.lock();
    	try{
    		long nextPosition = position + size;
    		long ret = position;
    		if(position >limit){
    			if(nextPosition < capacity){
    				position = nextPosition;    				
    			}else if(nextPosition == capacity){
    				position =0;
    			}else if(size < limit){
    				position = size;
    				ret = 0;
    			}
    		}else if(position < limit){
    			if(nextPosition < limit || nextPosition == limit){
    				position = nextPosition;
    			}else {
    				ret = -1;
    			}
    		}else{
    			ret = -1;
    		}
    		return ret;
    	}finally{
    		lock.unlock();
    	}
    }
    
    public void limit(long limit){
    	lock.lock();
    	try{
    		this.limit = limit;
    	}finally{
    		lock.unlock();
    	}
    }
    public void capacity(long capacity){
    	lock.lock();
    	try{
    		this.capacity = capacity;
    	}finally{
    		lock.unlock();
    	}
    }
    public long limit(){
      	lock.lock();
    	try{
    	return this.limit;
    	}finally{
    		lock.unlock();
    	}
    }
    public long capacity(){
      	lock.lock();
    	try{
    		return this.capacity;
    	}finally{
    		lock.unlock();
    	}
    }
    public long position(){
      	lock.lock();
    	try{
    		return this.position;
    	}finally{
    		lock.unlock();
    	}
    }
    	
    

}
