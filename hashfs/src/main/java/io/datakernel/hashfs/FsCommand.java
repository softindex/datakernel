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

package io.datakernel.hashfs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.datakernel.serializer.GsonSubclassesAdapter;

import java.util.Set;

import static java.util.Collections.unmodifiableSet;

abstract class FsCommand {
	public static Gson getGSON() {
		return new GsonBuilder()
				.registerTypeAdapter(FsCommand.class, GsonSubclassesAdapter.builder()
						.subclassField("commandType")
						.subclass("Upload", Upload.class)
						.subclass("Commit", Commit.class)
						.subclass("Download", Download.class)
						.subclass("Delete", Delete.class)
						.subclass("List", List.class)
						.subclass("Alive", Alive.class)
						.subclass("Offer", Offer.class)
						.build())
				.setPrettyPrinting()
				.enableComplexMapKeySerialization()
				.create();
	}

	static class Upload extends FsCommand {

		public final String filePath;

		public Upload(String filePath) {
			this.filePath = filePath;
		}

		@Override
		public String toString() {
			return "Upload{filepath=\'" + filePath + "\'}";
		}

	}

	static class Commit extends FsCommand {
		public final String filePath;

		public final boolean isOk;

		public Commit(String filePath, boolean isOk) {
			this.filePath = filePath;
			this.isOk = isOk;
		}

		@Override
		public String toString() {
			return "Commit{filepath=\'" + filePath + "\',isOk=" + isOk + "}";
		}

	}

	static class Delete extends FsCommand {

		public final String filePath;

		public Delete(String filePath) {
			this.filePath = filePath;
		}

		@Override
		public String toString() {
			return "Delete{filepath=\'" + filePath + "\'}";
		}

	}

	static class Download extends FsCommand {

		public final String filePath;

		public Download(String filePath) {
			this.filePath = filePath;
		}

		@Override
		public String toString() {
			return "Delete{filepath=\'" + filePath + "\'}";
		}

	}

	static class List extends FsCommand {
		@Override
		public String toString() {
			return "List{all files}";
		}

	}

	static class Alive extends FsCommand {
		@Override
		public String toString() {
			return "Alive{servers}";
		}
	}

	static class Offer extends FsCommand {
		public final Set<String> forDeletion;
		public final Set<String> forUpload;

		public Offer(Set<String> forDeletion, Set<String> forUpload) {
			this.forDeletion = unmodifiableSet(forDeletion);
			this.forUpload = unmodifiableSet(forUpload);
		}

		@Override
		public String toString() {
			return "Offer{forDeletion:" + forDeletion.size() + ",forUpload:" + forUpload.size() + "}";
		}
	}
}