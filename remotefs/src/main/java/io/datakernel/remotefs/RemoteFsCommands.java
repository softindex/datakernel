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

package io.datakernel.remotefs;

import com.google.gson.TypeAdapter;
import io.datakernel.util.gson.TypeAdapterObject;
import io.datakernel.util.gson.TypeAdapterObjectSubtype;

import java.util.Map;

import static io.datakernel.util.gson.GsonAdapters.*;

@SuppressWarnings("WeakerAccess")
public final class RemoteFsCommands {
	static final TypeAdapter<FsCommand> ADAPTER = TypeAdapterObjectSubtype.<FsCommand>create()
		.withSubtype(Upload.class, TypeAdapterObject.create(Upload::new)
			.with("fileName", STRING_JSON, Upload::getFileName, Upload::setFileName)
			.with("offset", LONG_JSON, Upload::getOffset, Upload::setOffset))
		.withSubtype(Download.class, TypeAdapterObject.create(Download::new)
			.with("filePath", STRING_JSON, Download::getFileName, Download::setFileName)
			.with("offset", LONG_JSON, Download::getOffset, Download::setOffset)
			.with("length", LONG_JSON, Download::getLength, Download::setLength))
		.withSubtype(Move.class, TypeAdapterObject.create(Move::new)
			.with("changes", ofMap(STRING_JSON), Move::getChanges, Move::setChanges))
		.withSubtype(Copy.class, TypeAdapterObject.create(Copy::new)
			.with("changes", ofMap(STRING_JSON), Copy::getChanges, Copy::setChanges))
		.withSubtype(List.class, TypeAdapterObject.create(List::new)
			.with("glob", STRING_JSON, List::getGlob, List::setGlob))
		.withSubtype(Delete.class, TypeAdapterObject.create(Delete::new)
			.with("glob", STRING_JSON, Delete::getGlob, Delete::setGlob));

	public static abstract class FsCommand {
	}

	public static final class Upload extends FsCommand {
		private String fileName;
		private long offset;

		public Upload() {
		}

		public Upload(String fileName, long offset) {
			this.fileName = fileName;
			this.offset = offset;
		}

		public String getFileName() {
			return fileName;
		}

		public void setFileName(String fileName) {
			this.fileName = fileName;
		}

		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}

		@Override
		public String toString() {
			return "Upload{fileName='" + fileName + "'}";
		}
	}

	public static final class Download extends FsCommand {
		private String fileName;
		private long offset;
		private long length;

		public Download() {
		}

		public Download(String fileName, long offset, long length) {
			this.fileName = fileName;
			this.offset = offset;
			this.length = length;
		}

		public String getFileName() {
			return fileName;
		}

		public void setFileName(String fileName) {
			this.fileName = fileName;
		}

		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}

		public long getLength() {
			return length;
		}

		public void setLength(long length) {
			this.length = length;
		}

		@Override
		public String toString() {
			return "Download{fileName='" + fileName + "', offset=" + offset + ", length=" + length + '}';
		}
	}

	public static final class Move extends FsCommand {
		private Map<String, String> changes;

		public Move() {
		}

		public Move(Map<String, String> changes) {
			this.changes = changes;
		}

		public Map<String, String> getChanges() {
			return changes;
		}

		public void setChanges(Map<String, String> changes) {
			this.changes = changes;
		}

		@Override
		public String toString() {
			return "Move{changes=" + changes + '}';
		}
	}

	public static final class Copy extends FsCommand {
		private Map<String, String> changes;

		public Copy() {
		}

		public Copy(Map<String, String> changes) {
			this.changes = changes;
		}

		public Map<String, String> getChanges() {
			return changes;
		}

		public void setChanges(Map<String, String> changes) {
			this.changes = changes;
		}

		@Override
		public String toString() {
			return "Copy{changes=" + changes + '}';
		}
	}

	public static final class List extends FsCommand {
		private String glob;

		public List() {
		}

		public List(String glob) {
			this.glob = glob;
		}

		public String getGlob() {
			return glob;
		}

		public void setGlob(String glob) {
			this.glob = glob;
		}

		@Override
		public String toString() {
			return "List{glob='" + glob + "'}";
		}
	}

	public static final class Delete extends FsCommand {
		private String glob;

		public Delete() {
		}

		public Delete(String glob) {
			this.glob = glob;
		}

		public String getGlob() {
			return glob;
		}

		public void setGlob(String glob) {
			this.glob = glob;
		}

		@Override
		public String toString() {
			return "Delete{glob='" + glob + "'}";
		}
	}
}
