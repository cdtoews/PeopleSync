package edu.mit.ist.logging;
import org.apache.logging.log4j.*;
import org.apache.logging.log4j.core.*;
import org.apache.logging.log4j.core.appender.*;
import org.apache.logging.log4j.core.config.*;
import org.apache.logging.log4j.core.layout.PatternLayout;
import java.io.*;
/**
 * @author andrew.flower
 */
public class StringAppender extends AbstractOutputStreamAppender<StringAppender.StringOutputStreamManager> {
	static LoggerContext context = (LoggerContext) LogManager.getContext(false);
	static Configuration configuration = context.getConfiguration();
	StringOutputStreamManager manager;

	private StringAppender(String name, Layout<? extends Serializable> layout, Filter filter, StringOutputStreamManager manager, boolean ignoreExceptions, boolean immediateFlush) {
		super(name, layout, filter, ignoreExceptions, immediateFlush, manager);
		this.manager = manager;
	}

	public static StringAppender createStringAppender(String nullablePatternString) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		PatternLayout layout;
		if (nullablePatternString == null) {
			layout = PatternLayout.createDefaultLayout();
		} else {
			//ctoews: added the second parameter to match log4j2 2.4.1
			layout = PatternLayout.createLayout(nullablePatternString, null, configuration, null, null, true, false, null, null);
			//layout = PatternLayout.createLayout(nullablePatternString, configuration, null, null, true, false, null, null);
		}

		return new StringAppender(
				"StringAppender",
				layout,
				null,
				new StringOutputStreamManager(outputStream, "StringStream", layout),
				false,
				true);
	}

	public void addToLogger(String loggerName, Level level) {
		LoggerConfig loggerConfig = configuration.getLoggerConfig(loggerName);
		loggerConfig.addAppender(this, level, null);
		context.updateLoggers();
	}

	public void removeFromLogger(String loggerName) {
		LoggerConfig loggerConfig = configuration.getLoggerConfig(loggerName);
		loggerConfig.removeAppender("StringAppender");
		context.updateLoggers();
	}

	public String getOutput() {
		manager.flush();
		return new String(manager.getStream().toByteArray());
	}

	/**
	 * StringOutputStreamManager to manage an in memory bytestream representing our stream
	 */
	protected static class StringOutputStreamManager extends OutputStreamManager {
		ByteArrayOutputStream stream;

		protected StringOutputStreamManager(ByteArrayOutputStream os, String streamName, Layout<?> layout) {
			//ctoews: added last parameter to play nice with 2.4.1
			super(os, streamName, layout,false);
			
			stream = os;
		}

		public ByteArrayOutputStream getStream() {
			return stream;
		}
	}
}
