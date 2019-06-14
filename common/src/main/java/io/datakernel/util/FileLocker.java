/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.logging.Logger;

import static io.datakernel.util.Preconditions.check;
import static java.util.logging.Level.SEVERE;

public final class FileLocker {
	private static final Logger logger = Logger.getLogger(FileLocker.class.getName());

	synchronized public static FileLocker obtainLockOrDie(String filename) {
		FileLocker fileLocker = new FileLocker(filename);
		if (!fileLocker.obtainLock()) {
			logger.log(SEVERE, () -> "Could not obtain lock for " + filename);
			throw new RuntimeException("Could not obtain lock");
		}
		return fileLocker;
	}

	private final File lockFile;
	@Nullable
	private FileOutputStream lockStream;

	@Nullable
	private FileLock fileLock;

	private FileLocker(String lockFile) {
		this(new File(lockFile));
	}

	private FileLocker(File lockFile) {
		this.lockFile = lockFile.getAbsoluteFile();
	}

	synchronized public void obtainLockOrDie() {
		if (!obtainLock()) {
			logger.log(SEVERE, () -> "Could not obtain lock for " + this);
			throw new RuntimeException("Could not obtain lock");
		}
	}

	synchronized public boolean obtainLock() {
		try {
			File parentDir = lockFile.getCanonicalFile().getParentFile();
			if (parentDir != null) {
				check(parentDir.mkdirs(), "Cannot create directory %s", parentDir);
			}
			lockStream = new FileOutputStream(lockFile);
			lockStream.write(0);
			fileLock = lockStream.getChannel().tryLock();
			return fileLock != null;
		} catch (IOException e) {
			logger.log(SEVERE, e, () -> "IO Exception during locking" + lockFile);
			return false;
		}
	}

	synchronized public void releaseLock() {
		try {
			if (fileLock != null) {
				fileLock.release();
				fileLock = null;
			}
		} catch (IOException e) {
			logger.log(SEVERE, e, () -> "IO Exception during releasing FileLock on " + lockFile);
		}
		try {
			if (lockStream != null) {
				lockStream.close();
				lockStream = null;
			}
		} catch (IOException e) {
			logger.log(SEVERE, e, () -> "IO Exception during closing FileOutputStream on " + lockFile);
		}
	}

	public boolean isLocked() {
		return fileLock != null;
	}

	@Override
	public String toString() {
		return lockFile.toString();
	}

}
