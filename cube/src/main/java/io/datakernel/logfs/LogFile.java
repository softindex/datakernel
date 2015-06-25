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

package io.datakernel.logfs;

public final class LogFile implements Comparable<LogFile> {
	private final String name;
	private final int n;

	public LogFile(String name, int n) {
		this.name = name;
		this.n = n;
	}

	@Override
	public int compareTo(LogFile o) {
		int i = name.compareTo(o.name);
		if (i != 0)
			return i;
		return Integer.compare(n, o.n);
	}

	public String getName() {
		return name;
	}

	public int getN() {
		return n;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LogFile logFile = (LogFile) o;

		if (n != logFile.n) return false;
		if (!this.name.equals(logFile.name)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = name.hashCode();
		result = 31 * result + n;
		return result;
	}

	@Override
	public String toString() {
		return "{" + name + "_" + n + '}';
	}
}
