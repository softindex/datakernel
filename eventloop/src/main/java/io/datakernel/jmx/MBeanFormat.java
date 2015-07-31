/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.jmx;

import static com.google.common.base.Throwables.propagate;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.io.CharStreams;

public final class MBeanFormat {
	private static final Splitter SPLITTER_LN = Splitter.on('\n');

	private MBeanFormat() {
	}

	public static ObjectName name(Class<?> type) {
		return name(type.getPackage().getName(), type.getSimpleName());
	}

	public static ObjectName name(String domain, String type, Class<?> valueType) {
		return name(domain, type, valueType.getSimpleName());
	}

	public static ObjectName name(String domain, String type, String value) {
		value = value.replace(':', '_').replace(',', '_').replace('=', '_');
		return name(domain + ":type=" + type + ",value=" + value);
	}

	public static ObjectName name(String domain, String type) {
		return name(domain + ":type=" + type);
	}

	private static ObjectName name(String name) {
		try {
			return new ObjectName(name);
		} catch (MalformedObjectNameException e) {
			throw propagate(e);
		}
	}

	public static String[] formatException(Throwable exception) {
		if (exception == null)
			return null;
		StringBuilder sb = new StringBuilder();
		exception.printStackTrace(new PrintWriter(CharStreams.asWriter(sb)));
		return formatMultilines(sb.toString());
	}

	private static String formatHours(long period) {
		long milliseconds = period % 1000;
		long seconds = (period / 1000) % 60;
		long minutes = (period / (60 * 1000)) % 60;
		long hours = period / (60 * 60 * 1000);
		return String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", seconds) + "." + String.format("%03d", milliseconds);
	}

	public static String formatDuration(long period) {
		if (period == 0)
			return "";
		return formatHours(period);
	}

	public static String formatPeriodAgo(long timestamp) {
		if (timestamp == 0)
			return "Never";
		return formatHours(System.currentTimeMillis() - timestamp) + " ago";
	}

	public static String formatDateTime(long timestamp) {
		if (timestamp == 0)
			return null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return dateFormat.format(new Date(timestamp));
	}

	public static String[] formatMultilines(String s) {
		if (s == null)
			return null;
		return Iterables.toArray(SPLITTER_LN.split(s), String.class);
	}

}
