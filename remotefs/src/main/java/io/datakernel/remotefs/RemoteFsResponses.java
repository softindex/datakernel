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

import com.google.gson.TypeAdapter;
import io.datakernel.json.TypeAdapterObject;
import io.datakernel.json.TypeAdapterObjectSubtype;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static io.datakernel.json.GsonAdapters.*;

public final class RemoteFsResponses {
	public static final TypeAdapter<FileMetadata> FILE_META_JSON = transform(
			ofHeterogeneousArray(new TypeAdapter<?>[]{STRING_JSON, LONG_JSON, LONG_JSON}),
			data -> new FileMetadata((String) data[0], (long) data[1], (long) data[2]),
			meta -> new Object[]{meta.getFilename(), meta.getSize(), meta.getTimestamp()}
	);

	static final TypeAdapter<FsResponse> ADAPTER = TypeAdapterObjectSubtype.<FsResponse>create()
			.withSubtype(UploadFinished.class, TypeAdapterObject.create(UploadFinished::new))
			.withSubtype(DownloadSize.class, TypeAdapterObject.create(DownloadSize::new)
					.with("size", LONG_JSON, DownloadSize::getSize, DownloadSize::setSize))
			.withSubtype(MoveFinished.class, TypeAdapterObject.create(MoveFinished::new)
					.with("moved", ofSet(STRING_JSON), MoveFinished::getMoved, MoveFinished::setMoved))
			.withSubtype(CopyFinished.class, TypeAdapterObject.create(CopyFinished::new)
					.with("copied", ofSet(STRING_JSON), CopyFinished::getCopied, CopyFinished::setCopied))
			.withSubtype(ListFinished.class, TypeAdapterObject.create(ListFinished::new)
					.with("files", ofList(FILE_META_JSON), ListFinished::getFiles, ListFinished::setFiles))
			.withSubtype(DeleteFinished.class, TypeAdapterObject.create(DeleteFinished::new))
			.withSubtype(ServerError.class, TypeAdapterObject.create(ServerError::new)
					.with("message", STRING_JSON, ServerError::getMessage, ServerError::setMessage));

	public static abstract class FsResponse {
	}

	public static class UploadFinished extends FsResponse {
		@Override
		public String toString() {
			return "UploadFinished{}";
		}
	}

	public static class DownloadSize extends FsResponse {
		private long size;

		public DownloadSize() {
		}

		public DownloadSize(long size) {
			this.size = size;
		}

		public long getSize() {
			return size;
		}

		public void setSize(long size) {
			this.size = size;
		}

		@Override
		public String toString() {
			return "DownloadSize{size=" + size + '}';
		}
	}

	public static class MoveFinished extends FsResponse {
		private Set<String> moved;

		public MoveFinished() {
		}

		public MoveFinished(Set<String> moved) {
			this.moved = moved;
		}

		public Set<String> getMoved() {
			return moved;
		}

		public void setMoved(Set<String> moved) {
			this.moved = moved;
		}

		@Override
		public String toString() {
			return "MoveFinished{moved=" + moved + '}';
		}
	}

	public static class CopyFinished extends FsResponse {
		private Set<String> copied;

		public CopyFinished() {
		}

		public CopyFinished(Set<String> copied) {
			this.copied = copied;
		}

		public Set<String> getCopied() {
			return copied;
		}

		public void setCopied(Set<String> copied) {
			this.copied = copied;
		}

		@Override
		public String toString() {
			return "CopyFinished{copied=" + copied + '}';
		}
	}

	public static class ListFinished extends FsResponse {
		private List<FileMetadata> files;

		public ListFinished() {
		}

		public ListFinished(List<FileMetadata> files) {
			this.files = Collections.unmodifiableList(files);
		}

		public List<FileMetadata> getFiles() {
			return files;
		}

		public void setFiles(List<FileMetadata> files) {
			this.files = files;
		}

		@Override
		public String toString() {
			return "ListFinished{files=" + files.size() + '}';
		}
	}

	public static class DeleteFinished extends FsResponse {
		public DeleteFinished() {
		}

		public DeleteFinished(Void $) { // so we can use lambda-constructor
		}

		@Override
		public String toString() {
			return "DeleteFinished{}";
		}
	}

	public static class ServerError extends FsResponse {
		private String message;

		public ServerError() {
		}

		public ServerError(String message) {
			this.message = message;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

		@Override
		public String toString() {
			return "ServerError{message=" + message + '}';
		}
	}
}
