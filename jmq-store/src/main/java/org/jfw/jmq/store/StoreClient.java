package org.jfw.jmq.store;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

import org.jfw.jmq.core.Broker;
import org.jfw.jmq.store.command.written.WriteCommand;

public class StoreClient {
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	private String clientId;
	private boolean inTrans;
	private Broker broker;
	public String getClientId() {
		return clientId;
	}
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	public Broker getBroker() {
		return broker;
	}
	public void setBroker(Broker broker) {
		this.broker = broker;
	}
	
	public StoreClient(Broker broker,String clientId){
		this.broker = broker;
		this.clientId = clientId;
	}
	public Topic getCurrentTopic() {
		// TODO Auto-generated method stub
		return null;
	}
	public AsynchronousSocketChannel getChannel() {
		// TODO Auto-generated method stub
		return null;
	}
	public ByteBuffer allocateBuf() {
		// TODO Auto-generated method stub
		return null;
	}
	public void over(WriteCommand writeCommand) {
		//		TODO Auto-generated method stub
		
	}
	
	
	
//	AsynchronousSocketChannel getChannel();
//	ByteBuffer allocateBuf(); 
//	void free(ByteBuffer buf);
//	Topic getCurrentTopic();
//	void over(Command cmd);
}
