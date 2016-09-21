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

import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;

import java.util.List;

public abstract class AbstractLogFileSystem implements LogFileSystem {
	protected static final class PartitionAndFile {
		public final String logPartition;
		public final LogFile logFile;

		private PartitionAndFile(String logPartition, LogFile logFile) {
			this.logPartition = logPartition;
			this.logFile = logFile;
		}
	}

	protected static PartitionAndFile parse(String s) {
		int index1 = s.indexOf('.');
		if (index1 == -1)
			return null;
		String name = s.substring(0, index1);
		if (name.isEmpty())
			return null;
		s = s.substring(index1 + 1);
		if (!s.endsWith(".log"))
			return null;
		s = s.substring(0, s.length() - 4);
		int n = 0;
		int index2 = s.indexOf('-');
		String logPartition;
		if (index2 != -1) {
			logPartition = s.substring(0, index2);
			try {
				n = Integer.parseInt(s.substring(index2 + 1));
			} catch (NumberFormatException e) {
				return null;
			}
		} else {
			logPartition = s;
		}
		if (logPartition.isEmpty())
			return null;
		return new PartitionAndFile(logPartition, new LogFile(name, n));
	}

	protected static String fileName(String logPartition, LogFile logFile) {
		return logFile.getName() + "." + logPartition + (logFile.getN() != 0 ? "-" + logFile.getN() : "") + ".log";
	}

	@Override
	public void makeUniqueLogFile(String logPartition, final String logName, final ResultCallback<LogFile> callback) {
		list(logPartition, new ForwardingResultCallback<List<LogFile>>(callback) {
			@Override
			protected void onResult(List<LogFile> logFiles) {
				int chunkN = 0;
				for (LogFile logFile : logFiles) {
					if (logFile.getName().equals(logName)) {
						chunkN = Math.max(chunkN, logFile.getN() + 1);
					}
				}
				callback.sendResult(new LogFile(logName, chunkN));
			}
		});
	}
}
