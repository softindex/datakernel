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

public final class LogPosition {
	private final LogFile logFile;
	private final long position;

	private LogPosition(LogFile logFile, long position) {
		this.logFile = logFile;
		this.position = position;
	}

	private LogPosition() {
		this.logFile = null;
		this.position = 0L;
	}

	public static LogPosition create() {return new LogPosition();}

	public static LogPosition create(LogFile logFile, long position) {return new LogPosition(logFile, position);}

	public boolean isBeginning() {
		return position == 0L;
	}

	public LogFile getLogFile() {
		return logFile;
	}

	public long getPosition() {
		return position;
	}

	@Override
	public String toString() {
		return "LogPosition{logFile=" + logFile + ", position=" + position + '}';
	}
}
