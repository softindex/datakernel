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

import java.util.function.Function;

public final class MemSize {
	public static final long KB = 1024;
	public static final long MB = 1024 * KB;
	public static final long GB = 1024 * MB;
	public static final long TB = 1024 * GB;

	private final long bytes;

	private MemSize(long bytes) {
		this.bytes = bytes;
	}

	public static MemSize of(long bytes) {
		return new MemSize(bytes);
	}

	public static MemSize bytes(long bytes) {
		return MemSize.of(bytes);
	}

	public static MemSize kilobytes(long kilobytes) {
		return of(kilobytes * KB);
	}

	public static MemSize megabytes(long megabytes) {
		return of(megabytes * MB);
	}

	public static MemSize gigabytes(long gigabytes) {
		return of(gigabytes * GB);
	}

	public static MemSize terabytes(long terabytes) {
		return of(terabytes * TB);
	}

	public long toLong() {
		return bytes;
	}

	public int toInt() {
		if (bytes > Integer.MAX_VALUE) throw new IllegalStateException("MemSize exceeds Integer.MAX_VALUE: " + bytes);
		return (int) bytes;
	}

	public MemSize map(Function<Long, Long> fn) {
		return MemSize.of(fn.apply(bytes));
	}

	public static MemSize valueOf(String string) {
		return StringFormatUtils.parseMemSize(string);
	}

	public String format() {
		return StringFormatUtils.formatMemSize(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		MemSize memSize = (MemSize) o;

		return bytes == memSize.bytes;
	}

	@Override
	public String toString() {
		return "" + toLong() + "b";
	}
}
