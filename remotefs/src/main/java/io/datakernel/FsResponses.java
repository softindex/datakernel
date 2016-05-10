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

package io.datakernel;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.serializer.GsonSubclassesAdapter;

import java.util.Collections;
import java.util.List;

@SuppressWarnings("WeakerAccess")
public abstract class FsResponses {
	static Gson responseGson = new GsonBuilder()
			.registerTypeAdapter(FsResponse.class, GsonSubclassesAdapter.builder()
					.subclassField("commandType")
					.subclass("Error", Err.class)
					.subclass("FileList", ListOfFiles.class)
					.subclass("ResponseOk", Ok.class)
					.subclass("Acknowledge", Acknowledge.class)
					.subclass("ReadyBytes", Ready.class)
					.build())
			.setPrettyPrinting()
			.enableComplexMapKeySerialization()
			.create();

	public static abstract class FsResponse {

	}

	public static class Acknowledge extends FsResponse {
		@Override
		public String toString() {
			return "Done{OK}";
		}
	}

	public static class Ready extends FsResponse {
		public final long size;

		public Ready(long size) {
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
		public final String msg;

		public Err(String msg) {
			this.msg = msg;
		}

		@Override
		public String toString() {
			return "Error{" + msg + "}";
		}
	}

	public static class ListOfFiles extends FsResponse {
		public final List<String> files;

		public ListOfFiles(List<String> files) {
			this.files = Collections.unmodifiableList(files);
		}

		@Override
		public String toString() {
			return "Listed{" + files.size() + "}";
		}
	}
}