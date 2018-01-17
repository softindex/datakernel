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

import java.util.Collections;
import java.util.List;

import static io.datakernel.util.gson.GsonAdapters.*;

@SuppressWarnings("WeakerAccess")
public final class RemoteFsResponses {
	static TypeAdapter<FsResponse> adapter = TypeAdapterObjectSubtype.<FsResponse>create()
		.withSubtype(Err.class, "Error", TypeAdapterObject.create(Err::new)
			.with("msg", STRING_JSON, Err::getMsg, Err::setMsg))
		.withSubtype(ListOfFiles.class, "FileList", TypeAdapterObject.create(ListOfFiles::new)
			.with("files", ofList(STRING_JSON), ListOfFiles::getFiles, ListOfFiles::setFiles))
		.withSubtype(Ready.class, "ReadyBytes", TypeAdapterObject.create(Ready::new)
			.with("size", LONG_JSON, Ready::getSize, Ready::setSize))
		.withStatelessSubtype(Ok::new, "ResponseOk")
		.withStatelessSubtype(Acknowledge::new, "Acknowledge");

	public static abstract class FsResponse {

	}

	public static class Acknowledge extends FsResponse {
		@Override
		public String toString() {
			return "Done{OK}";
		}
	}

	public static class Ready extends FsResponse {
		private long size;

		public Ready() {
		}

		public Ready(long size) {
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
			return "Ready{" + size + "}";
		}
	}

	public static class Ok extends FsResponse {
		@Override
		public String toString() {
			return "Operation{OK}";
		}
	}

	public static class Err extends FsResponse {
		private String msg;

		public Err() {
		}

		public Err(String msg) {
			this.msg = msg;
		}

		public String getMsg() {
			return msg;
		}

		public void setMsg(String msg) {
			this.msg = msg;
		}

		@Override
		public String toString() {
			return "Error{" + msg + "}";
		}
	}

	public static class ListOfFiles extends FsResponse {
		private List<String> files;

		public ListOfFiles() {
		}

		public ListOfFiles(List<String> files) {
			this.files = Collections.unmodifiableList(files);
		}

		public List<String> getFiles() {
			return files;
		}

		public void setFiles(List<String> files) {
			this.files = files;
		}

		@Override
		public String toString() {
			return "Listed{" + files.size() + "}";
		}
	}
}