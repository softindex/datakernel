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

import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

import static io.datakernel.util.Preconditions.checkArgument;

public final class MemSize {
	public static final long KB = 1024;
	public static final long MB = 1024 * KB;
	public static final long GB = 1024 * MB;
	public static final long TB = 1024 * GB;

	private final long bytes;

	private MemSize(long bytes) {
		this.bytes = bytes;
	}

	@NotNull
	public static MemSize of(long bytes) {
		checkArgument(bytes >= 0, "Cannot create MemSize of negative value");
		return new MemSize(bytes);
	}

	@NotNull
	public static MemSize bytes(long bytes) {
		return MemSize.of(bytes);
	}

	@NotNull
	public static MemSize kilobytes(long kilobytes) {
		checkArgument(kilobytes <= Long.MAX_VALUE / KB, "Resulting number of bytes exceeds Long.MAX_VALUE");
		return of(kilobytes * KB);
	}

	@NotNull
	public static MemSize megabytes(long megabytes) {
		checkArgument(megabytes <= Long.MAX_VALUE / MB, "Resulting number of bytes exceeds Long.MAX_VALUE");
		return of(megabytes * MB);
	}

	@NotNull
	public static MemSize gigabytes(long gigabytes) {
		checkArgument(gigabytes <= Long.MAX_VALUE / GB, "Resulting number of bytes exceeds Long.MAX_VALUE");
		return of(gigabytes * GB);
	}

	@NotNull
	public static MemSize terabytes(long terabytes) {
		checkArgument(terabytes <= Long.MAX_VALUE / TB, "Resulting number of bytes exceeds Long.MAX_VALUE");
		return of(terabytes * TB);
	}

	public long toLong() {
		return bytes;
	}

	public int toInt() {
		if (bytes > Integer.MAX_VALUE) throw new IllegalStateException("MemSize exceeds Integer.MAX_VALUE: " + bytes);
		return (int) bytes;
	}

	@NotNull
	public MemSize map(@NotNull Function<Long, Long> fn) {
		return MemSize.of(fn.apply(bytes));
	}

	@NotNull
	public static MemSize valueOf(@NotNull String string) {
		return StringFormatUtils.parseMemSize(string);
	}

	@NotNull
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

	@NotNull
	@Override
	public String toString() {
		return "" + toLong();
	}
}
