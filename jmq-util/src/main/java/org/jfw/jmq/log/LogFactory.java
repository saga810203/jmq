package org.jfw.jmq.log;

import java.util.ServiceLoader;

public class LogFactory {
	private static String classNameForLogFactory;
	private static volatile LogFactory defaule = new LogFactory();
	private static Logger defaultLog = new NoLogger();

	public Logger getLogger(Class<?> clazz) {
		return LogFactory.defaultLog;
	}

	public static Logger getLog(Class<?> clazz) {
		return LogFactory.defaule.getLogger(clazz);
	}

	static {
		try {
			ServiceLoader<LogFactory> loader = ServiceLoader.load(LogFactory.class);
			for (LogFactory fac : loader) {
				if (fac != null) {
					defaule = fac;
					break;
				}
			}
		} catch (Throwable thr) {
		}
	}
}
