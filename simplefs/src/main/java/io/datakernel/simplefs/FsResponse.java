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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.serializer.GsonSubclassesAdapter;

import java.util.Collections;
import java.util.Set;

abstract class FsResponse {
	static Gson getGSON() {
		return new GsonBuilder()
				.registerTypeAdapter(FsResponse.class, GsonSubclassesAdapter.builder()
						.subclassField("commandType")
						.subclass("Error", Error.class)
						.subclass("FileList", ListFiles.class)
						.subclass("ResponseOk", Ok.class)
						.subclass("Acknowledge", Acknowledge.class)
						.subclass("ReadyBytes", Ready.class)
						.build())
				.setPrettyPrinting()
				.enableComplexMapKeySerialization()
				.create();
	}

	static class Acknowledge extends FsResponse {
		@Override
		public String toString() {
			return "Done{OK}";
		}
	}

	static class Ready extends FsResponse {
		public final long size;

		public Ready(long size) {
			this.size = size;
		}

		@Override
		public String toString() {
			return "Ready{" + size + "}";
		}
	}

	static class Ok extends FsResponse {
		@Override
		public String toString() {
			return "Operation{OK}";
		}
	}

	static class Error extends FsResponse {
		public final String msg;

		public Error(String msg) {
			this.msg = msg;
		}

		@Override
		public String toString() {
			return "Error{" + msg + "}";
		}
	}

	static class ListFiles extends FsResponse {
		public final Set<String> files;

		public ListFiles(Set<String> files) {
			this.files = Collections.unmodifiableSet(files);
		}

		@Override
		public String toString() {
			return "Listed{" + files.size() + "}";
		}
	}
}