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

package io.datakernel.simplefs;

import io.datakernel.async.CompletionCallback;

import java.util.HashMap;
import java.util.Map;

import static io.datakernel.simplefs.FileStatusRegister.FileStatus.*;

class FileStatusRegister {
	private final Map<String, FileStatus> file2status = new HashMap<>();
	private final Map<String, Integer> pendingDownloadsStatus = new HashMap<>();
	private CompletionCallback callback;

	void executeOnUploadsComplete(CompletionCallback callback) {
		if (isEmpty()) {
			callback.onComplete();
		} else {
			this.callback = callback;
		}
	}

	boolean isRegistered(String fileName) {
		return file2status.containsKey(fileName);
	}

	boolean isApproved(String fileName) {
		return file2status.get(fileName) == APPROVED;
	}

	boolean isReady(String fileName) {
		return file2status.get(fileName) == READY;
	}

	boolean isUploading(String fileName) {
		return file2status.get(fileName) == UPLOADING;
	}

	public boolean isDownloading(String fileName) {
		return file2status.get(fileName) == DOWNLOADING;
	}

	boolean isEmpty() {
		return file2status.isEmpty();
	}

	void remove(String fileName) {
		file2status.remove(fileName);
		if (callback != null && file2status.isEmpty())
			callback.onComplete();
	}

	void ensureStatus(String fileName, FileStatus state) {
		file2status.put(fileName, state);
	}

	public boolean isModified(String fileName) {
		FileStatus status = file2status.get(fileName);
		return status != null && status != DOWNLOADING;
	}

	public void onStartDownload(String fileName) {
		Integer downloadsQuantity = pendingDownloadsStatus.get(fileName);
		if (downloadsQuantity == null) {
			pendingDownloadsStatus.put(fileName, 1);
		} else {
			pendingDownloadsStatus.put(fileName, downloadsQuantity + 1);
		}
	}

	public void onEndDownload(String fileName) {
		Integer downloadsQuantity = pendingDownloadsStatus.get(fileName);
		if (downloadsQuantity == null) {
			throw new RuntimeException("This file is not being downloaded now");
		} else {
			if (downloadsQuantity == 1) {
				remove(fileName);
			}
			pendingDownloadsStatus.put(fileName, downloadsQuantity - 1);
		}
	}

	enum FileStatus {
		READY, APPROVED, UPLOADING, DOWNLOADING
	}
}
