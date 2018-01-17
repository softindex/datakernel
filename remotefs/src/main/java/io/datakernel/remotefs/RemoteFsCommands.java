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

import io.datakernel.utils.JsonSerializer;
import io.datakernel.utils.TypeAdapterObject;
import io.datakernel.utils.TypeAdapterObjectSubtype;

import java.util.Map;

import static io.datakernel.utils.GsonAdapters.*;

@SuppressWarnings("WeakerAccess")
public final class RemoteFsCommands {

	static JsonSerializer<FsCommand> serializer = new JsonSerializer<>(TypeAdapterObjectSubtype.<FsCommand>create()
		.withSubtype(Upload.class, "Upload", TypeAdapterObject.create(Upload::new)
			.with("filePath", STRING_JSON, Upload::getFilePath, Upload::setFilePath))
		.withSubtype(Download.class, "Download", TypeAdapterObject.create(Download::new)
			.with("filePath", STRING_JSON, Download::getFilePath, Download::setFilePath)
			.with("startPosition", LONG_JSON, Download::getStartPosition, Download::setStartPosition))
		.withSubtype(Delete.class, "Delete", TypeAdapterObject.create(Delete::new)
			.with("filePath", STRING_JSON, Delete::getFilePath, Delete::setFilePath))
		.withStatelessSubtype(ListFiles::new, "List")
		.withSubtype(Move.class, "Move", TypeAdapterObject.create(Move::new)
			.with("changes", ofMap(STRING_JSON), Move::getChanges, Move::setChanges)));

	public static abstract class FsCommand {}

	public static final class Upload extends FsCommand {

		private String filePath;

		public Upload() {}

		public Upload(String filePath) {
			this.filePath = filePath;
		}

		public String getFilePath() {
			return filePath;
		}

		public void setFilePath(String filePath) {
			this.filePath = filePath;
		}

		@Override
		public String toString() {
			return "Upload{filepath=\'" + getFilePath() + "\'}";
		}
	}

	public static final class Delete extends FsCommand {

		private String filePath;

		public Delete() {}

		public Delete(String filePath) {
			this.filePath = filePath;
		}

		public String getFilePath() {
			return filePath;
		}

		public void setFilePath(String filePath) {
			this.filePath = filePath;
		}

		@Override
		public String toString() {
			return "Delete{filepath=\'" + getFilePath() + "\'}";
		}
	}

	public static final class Download extends FsCommand {

		private String filePath;
		private long startPosition;

		public Download() {}

		public Download(String filePath, long startPosition) {
			this.filePath = filePath;
			this.startPosition = startPosition;
		}

		public String getFilePath() {
			return filePath;
		}

		public void setFilePath(String filePath) {
			this.filePath = filePath;
		}

		public long getStartPosition() {
			return startPosition;
		}

		public void setStartPosition(long startPosition) {
			this.startPosition = startPosition;
		}

		@Override
		public String toString() {
			return "Download{filepath=\'" + filePath + "\',startPosition=" + startPosition + "}";
		}
	}

	public static final class Move extends FsCommand {

		private Map<String, String> changes;

		public Move() {}

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

	public static final class ListFiles extends FsCommand {

		@Override
		public String toString() {
			return "List{all files}";
		}
	}
}