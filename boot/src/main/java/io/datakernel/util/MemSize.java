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

package io.datakernel.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.datakernel.util.Preconditions.checkArgument;

public final class MemSize {
	private static final Pattern PATTERN = Pattern.compile("([0-9]+([\\.][0-9]+)?)\\s*(|K|M|G|T)B?", Pattern.CASE_INSENSITIVE);
	private final long bytes;

	public MemSize(long bytes) {
		checkArgument(bytes >= 0, "Bytes must be positive or zero");
		this.bytes = bytes;
	}

	public long getBytes() {
		return bytes;
	}

	public static MemSize valueOf(String string) {
		Matcher matcher = PATTERN.matcher(string);
		if (matcher.matches()) {
			try {
				double value = Double.valueOf(matcher.group(1));
				String units = matcher.group(3).toLowerCase();

				long unit;
				switch (units) {
					case "":
						unit = 1;
						break;
					case "k":
						unit = 1L << (10 * 1);
						break;
					case "m":
						unit = 1L << (10 * 2);
						break;
					case "g":
						unit = 1L << (10 * 3);
						break;
					case "t":
						unit = 1L << (10 * 4);
						break;
					default:
						throw new IllegalArgumentException("Illegal units: " + string);
				}
				long bytes = (long) (value * unit);
				return new MemSize(bytes);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Illegal number format: " + string, e);
			}
		} else {
			throw new IllegalArgumentException("Illegal format: " + string);
		}
	}

	@Override
	public String toString() {
		return format();
	}

	public String format() {
		long bytes = getBytes();
		if (bytes == 0) {
			return "0b";
		}

		long terabyte = 1L << (10 * 4);
		if ((bytes % terabyte) == 0) {
			return bytes / terabyte + "Tb";
		}

		long gigabyte = 1L << (10 * 3);
		if ((bytes % gigabyte) == 0) {
			return bytes / gigabyte + "Gb";
		}

		long megabyte = 1L << (10 * 2);
		if ((bytes % megabyte) == 0) {
			return bytes / megabyte + "Mb";
		}

		long kilobyte = 1L << (10 * 1);
		if ((bytes % kilobyte) == 0) {
			return bytes / kilobyte + "Kb";
		}

		return bytes + "b";
	}
}
