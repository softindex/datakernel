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

package io.datakernel.hashfs2;

final class FileInfo {
	private final String fileName;
	private final int fileSize;

	public FileInfo(String fileName, int fileSize) {
		this.fileName = fileName;
		this.fileSize = fileSize;
	}

	public int getFileSize() {
		return fileSize;
	}

	public String getFileName() {
		return fileName;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		FileInfo that = (FileInfo) o;
		return this.fileSize == that.fileSize && this.fileName.equals(that.fileName);
	}

	@Override
	public int hashCode() {
		return ((fileName.hashCode() * 7) + fileSize) * 13;
	}

	@Override
	public String toString() {
		return "File name: " + fileName + ", size:" + fileSize;
	}
}
