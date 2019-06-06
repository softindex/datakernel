package io.datakernel.logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.Optional;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class DatakernelLoggerFormatter extends Formatter {
	private final Date dat = new Date();
	private static final String FORMAT_PROP_KEY = "io.datakernel.logger.DatakernelLoggerFormatter.format";
	private static final String DEFAULT_FORMAT = "[%1$tF %1$tT], %2$-7s: [%3$s] - %4$-7s %5$s %n";
	private static final String format = getFormat();


	private static String getFormat() {
		String format = System.getProperty(FORMAT_PROP_KEY);
		return format == null ? DEFAULT_FORMAT : format;
	}

	public synchronized String format(LogRecord record) {
		dat.setTime(record.getMillis());
		int threadID = record.getThreadID();
		String threadName = getThread(threadID)
				.map(Thread::getName)
				.orElseGet(() -> "Thread id - " + threadID);

		String message = formatMessage(record);
		String throwable = stackTraceToString(record.getThrown());
		String level = record.getLevel().getName();
		return String.format(
				format,
				dat,
				level,
				threadName,
				message,
				throwable
		);
	}

	private Optional<Thread> getThread(long threadId) {
		return Thread.getAllStackTraces().keySet().stream()
				.filter(t -> t.getId() == threadId)
				.findFirst();
	}

	private String stackTraceToString(Throwable thrown) {
		String throwable = "";
		if (thrown != null) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			pw.println();
			thrown.printStackTrace(pw);
			pw.close();
			throwable = sw.toString();
		}
		return throwable;
	}
}
