package io.datakernel.test;

import io.datakernel.util.ApplicationSettings;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class SimpleLoggingFormatter extends Formatter {
	private static final String DEFAULT_FORMAT = "%1$tF %1$tT %2$s %3$s - %4$-7s %5$s %n";
	private static final String format;

	static {
		format = ApplicationSettings.getString(SimpleLoggingFormatter.class, "format", DEFAULT_FORMAT);
	}

	@Override
	public String format(LogRecord record) {
		String throwable = "";
		if (record.getThrown() != null) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			pw.println();
			record.getThrown().printStackTrace(pw);
			pw.close();
			throwable = sw.toString();
		}
		return String.format(format,
				new Date(),
				record.getLevel().getLocalizedName(),
				record.getLoggerName(),
				formatMessage(record),
				throwable);
	}
}
