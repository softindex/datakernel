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

package io.datakernel.remotefs;

import io.datakernel.codec.CodecSubtype;
import io.datakernel.codec.StructuredCodec;

import java.util.Map;

import static io.datakernel.codec.StructuredCodecs.*;

@SuppressWarnings("WeakerAccess")
public final class RemoteFsCommands {

	static final StructuredCodec<FsCommand> CODEC = CodecSubtype.<FsCommand>create()
			.with(Upload.class, object(Upload::new,
					"fileName", Upload::getFileName, STRING_CODEC,
					"offset", Upload::getOffset, LONG_CODEC))
			.with(Download.class, object(Download::new,
					"filePath", Download::getFileName, STRING_CODEC,
					"offset", Download::getOffset, LONG_CODEC,
					"length", Download::getLength, LONG_CODEC))
			.with(Move.class, object(Move::new,
					"changes", Move::getChanges, ofMap(STRING_CODEC, STRING_CODEC)))
			.with(Copy.class, object(Copy::new,
					"changes", Copy::getChanges, ofMap(STRING_CODEC, STRING_CODEC)))
			.with(List.class, object(List::new,
					"glob", List::getGlob, STRING_CODEC))
			.with(Delete.class, object(Delete::new,
					"glob", Delete::getGlob, STRING_CODEC));

	public static abstract class FsCommand {
	}

	public static final class Upload extends FsCommand {
		private final String fileName;
		private final long offset;

		public Upload(String fileName, long offset) {
			this.fileName = fileName;
			this.offset = offset;
		}

		public String getFileName() {
			return fileName;
		}

		public long getOffset() {
			return offset;
		}

		@Override
		public String toString() {
			return "Upload{fileName='" + fileName + "'}";
		}
	}

	public static final class Download extends FsCommand {
		private final String fileName;
		private final long offset;
		private final long length;

		public Download(String fileName, long offset, long length) {
			this.fileName = fileName;
			this.offset = offset;
			this.length = length;
		}

		public String getFileName() {
			return fileName;
		}

		public long getOffset() {
			return offset;
		}

		public long getLength() {
			return length;
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
