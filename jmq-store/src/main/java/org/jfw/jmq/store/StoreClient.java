package org.jfw.jmq.store;

import org.jfw.jmq.core.Broker;

public class StoreClient {
	
	
	public void 
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
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
	
	
	
//	AsynchronousSocketChannel getChannel();
//	ByteBuffer allocateBuf(); 
//	void free(ByteBuffer buf);
//	Topic getCurrentTopic();
//	void over(Command cmd);
}
