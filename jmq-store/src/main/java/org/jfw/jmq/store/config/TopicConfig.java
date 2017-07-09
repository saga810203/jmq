package org.jfw.jmq.store.config;

public class TopicConfig {
	/**
	 * 消息在生成到提交之前的最大存活时间
	 */
	private long maxSurvivalTime;

	public long getMaxSurvivalTime() {
		return maxSurvivalTime;
	}

	public void setMaxSurvivalTime(long maxSurvivalTime) {
		this.maxSurvivalTime = maxSurvivalTime;
	}
	
	
	

}
