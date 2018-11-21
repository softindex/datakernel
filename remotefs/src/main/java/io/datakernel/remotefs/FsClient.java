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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.StacklessException;

import java.io.File;
import java.util.*;

/**
 * This interface represents a simple filesystem client with upload, download, move, delete and list operations.
 */
public interface FsClient {
	/**
	 * Returns a consumer of bytebufs which are written (or sent) to the file.
	 * <p>
	 * Void result is a marker, which is completed when uploading is complete.
	 * <p>
	 * So, outer promise might fail on connection try, end-of-stream promise
	 * might fail while uploading and result promise might fail when closing.
	 * <p>
	 * If offset is -1 then when file exists this will fail.
	 * If offset is 0 or more then this will override existing file starting from that byte
	 * and fail if file does not exist or is smaller than the offset.
	 *
	 * @param filename name of the file to upload
	 * @param offset   from which byte to write the uploaded data
	 * @return promise for stream consumer of byte buffers
	 */
	Promise<ChannelConsumer<ByteBuf>> upload(String filename, long offset);

	/**
	 * Shortcut for uploading NEW file
	 *
	 * @param filename name of the file to upload
	 * @return promise for stream consumer of byte buffers
	 */
	default Promise<ChannelConsumer<ByteBuf>> upload(String filename) {
		return upload(filename, -1);
	}

	/**
	 * Shortcut for first uploading the folder into a temporary folder and then atomically moving it out.
	 *
	 * @param filename   name of the file to upload
	 * @param tempFolder name of the temporary folder
	 * @return stream consumer of byte buffers
	 */
	default Promise<ChannelConsumer<ByteBuf>> upload(String filename, String tempFolder) {
		String tempName = tempFolder + File.separator + filename;
		return upload(tempName)
				.thenApply(consumer ->
						consumer.withAcknowledgement(ack ->
								ack.thenCompose($ -> move(tempName, filename))));
	}

	/**
	 * Shortcut which unwraps promise of consumer into a consumer that consumes when promise is complete.
	 * <p>
	 * It merges connection errors into end-of-stream promise, so on connection failure
	 * returned stream closes with the error.
	 *
	 * @param filename name of the file to upload
	 * @return stream consumer of byte buffers
	 */
	default ChannelConsumer<ByteBuf> uploader(String filename) {
		return ChannelConsumer.ofPromise(upload(filename));
	}

	default ChannelConsumer<ByteBuf> uploader(String filename, long offset) {
		return ChannelConsumer.ofPromise(upload(filename, offset));
	}

	/**
	 * Same shortcut, but for {@link #upload(String, String)}
	 */
	default ChannelConsumer<ByteBuf> uploader(String filename, String tempFolder) {
		return ChannelConsumer.ofPromise(upload(filename, tempFolder));
	}

	/**
	 * Returns a supplier of bytebufs which are read (or received) from the file.
	 * If file does not exist, or specified range goes beyond it's size,
	 * an error will be returned from the server.
	 *
	 * @param filename name of the file to be downloaded
	 * @param offset   from which byte to download the file
	 * @param length   how much bytes of the file do download
	 * @return promise for stream supplier of byte buffers
	 * @see #download(String, long)
	 * @see #download(String)
	 */
	Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset, long length);

	/**
	 * Shortcut for downloading the whole file from given offset.
	 *
	 * @return stream supplier of byte buffers
	 * @see #download(String, long, long)
	 * @see #download(String)
	 */
	default Promise<ChannelSupplier<ByteBuf>> download(String filename, long offset) {
		return download(filename, offset, -1);
	}

	/**
	 * Shortcut for downloading the whole available file.
	 *
	 * @param filename name of the file to be downloaded
	 * @return stream supplier of byte buffers
	 * @see #download(String, long)
	 * @see #download(String, long, long)
	 */
	default Promise<ChannelSupplier<ByteBuf>> download(String filename) {
		return download(filename, 0, -1);
	}

	/**
	 * Shortcut which unwraps promise of supplier into a supplier that produces when promise is complete
	 *
	 * @see #download(String, long, long)
	 */
	default ChannelSupplier<ByteBuf> downloader(String filename, long offset, long length) {
		return ChannelSupplier.ofPromise(download(filename, offset, length));
	}

	/**
	 * Same shortcut but for downloading the whole file from given offset.
	 *
	 * @see #download(String, long)
	 */
	default ChannelSupplier<ByteBuf> downloader(String filename, long offset) {
		return ChannelSupplier.ofPromise(download(filename, offset));
	}

	/**
	 * Same shortcut but for downloading the whole file.
	 *
	 * @param filename name of the file to be downloaded
	 * @see #download(String)
	 */
	default ChannelSupplier<ByteBuf> downloader(String filename) {
		return ChannelSupplier.ofPromise(download(filename));
	}

	/**
	 * Renames files by a given mapping.
	 *
	 * @param changes mapping from old file names to new file names
	 * @return promise of set of successfully moved files (using their original names)
	 */
	Promise<Set<String>> move(Map<String, String> changes);

	/**
	 * Shortcut for {@link #move} which will error if any of the files were not moved.
	 *
	 * @return marker promise as on success the returned set from move will always be same as <code>changes.keySet()</code>
	 */
	default Promise<Void> strictMove(Map<String, String> changes) {
		return move(changes)
				.thenCompose(res -> {
					if (res.size() < changes.size()) {
						Set<String> set = new HashSet<>(changes.keySet());
						set.removeAll(res);
						return Promise.ofException(new StacklessException(FsClient.class, "Those files were not moved: " + set));
					}
					return Promise.complete();
				});
	}

	/**
	 * Shortcut for {@link #strictMove} for a single file.
	 * By default is is equivalent to calling strictMove(Collections.singletonMap(fileName, newFileName))
	 *
	 * @param filename    file to be moved
	 * @param newFilename new file name
	 */
	default Promise<Void> move(String filename, String newFilename) {
		return strictMove(Collections.singletonMap(filename, newFilename));
	}

	/**
	 * Copies files by a given mapping.
	 *
	 * @param changes mapping from old file names to copy file names
	 * @return promise of set of successfully copied files (using their original names)
	 * @implNote RemoteFS is considered as an immutable fs, so at first copy will try to create a hard link instead.
	 */
	Promise<Set<String>> copy(Map<String, String> changes);

	/**
	 * Shortcut for {@link #copy} which will error if any of the files were not copied.
	 *
	 * @return marker promise as on success the returned set from move will always be same as <code>changes.keySet()</code>
	 */
	default Promise<Void> strictCopy(Map<String, String> changes) {
		return copy(changes)
				.thenCompose(res -> {
					if (res.size() < changes.size()) {
						Set<String> set = new HashSet<>(changes.keySet());
						set.removeAll(res);
						return Promise.ofException(new StacklessException(FsClient.class, "Those files were not copied: " + set));
					}
					return Promise.complete();
				});
	}

	/**
	 * Shortcut for {@link #strictCopy} for a single file.
	 * By default is is equivalent to calling strictCopy(Collections.singletonMap(fileName, newFileName))
	 *
	 * @param filename    file to be moved
	 * @param newFilename new file name
	 */
	default Promise<Void> copy(String filename, String newFilename) {
		return strictCopy(Collections.singletonMap(filename, newFilename));
	}

	/**
	 * Lists files that are matched by glob. Be sure to escape metachars if your filenames contain them
	 *
	 * @param glob specified in {@link java.nio.file.FileSystem#getPathMatcher NIO path matcher} documentation for glob patterns
	 * @return promise for fetched list of file descriptions
	 */
	Promise<List<FileMetadata>> list(String glob);

	/**
	 * Shortcut for {@link #list(String)} to list all files
	 */
	default Promise<List<FileMetadata>> list() {
		return list("**");
	}

	/**
	 * Shortcut for {@link #list(String)} to list all files in root directory
	 */
	default Promise<List<FileMetadata>> listLocal() {
		return list("*");
	}

	/**
	 * Shortcut for {@link #list(String)} with empty 'ping' list request
	 */
	default Promise<Void> ping() {
		return list("").toVoid();
	}

	/**
	 * Shrtcut to get Promise of {@link FileMetadata} of a single file.
	 *
	 * @param filename fileName of a file to fetch its metadata.
	 * @return promise of file description or <code>null</code>
	 */
	default Promise<FileMetadata> getMetadata(String filename) {
		return list(filename)
				.thenApply(list -> list.isEmpty() ? null : list.get(0));
	}

	/**
	 * Deletes files that are matched by glob. Be sure to escape metachars if your filenames contain them
	 *
	 * @param glob specified in {@link java.nio.file.FileSystem#getPathMatcher NIO path matcher} documentation for glob patterns
	 * @return promise for list of deleted files
	 */
	Promise<Void> delete(String glob);

	/**
	 * Creates a wrapper which will redirect all calls to it to a specific subfolder as if it was the root.
	 *
	 * @param folder folder to redirect calls to
	 * @return FsClient wrapper
	 */
	default FsClient subfolder(String folder) {
		return new SubfolderFsClient(this, folder);
	}
}
