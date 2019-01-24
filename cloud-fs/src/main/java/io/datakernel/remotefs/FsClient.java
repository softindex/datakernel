/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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
import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.exception.ConstantException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.util.CollectionUtils.map;
import static io.datakernel.util.FileUtils.escapeGlob;

/**
 * This interface represents a simple filesystem client with upload, download, move, delete and list operations.
 */
public interface FsClient {
	ConstantException FILE_NOT_FOUND = new ConstantException(FsClient.class, "File not found");

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
	 * Renames files by a given mapping.
	 *
	 * @param changes mapping from old file names to new file names
	 */
	default Promise<Void> moveBulk(Map<String, String> changes) {
		return Promises.all(changes.entrySet().stream().map(entry -> move(entry.getKey(), entry.getValue()).toTry()));
	}

	/**
	 * Shortcut for {@link #moveBulk} for a single file.
	 * By default is is equivalent to calling strictMove(Collections.singletonMap(fileName, newFileName))
	 *
	 * @param filename    file to be moved
	 * @param newFilename new file name
	 */
	default Promise<Void> move(String filename, String newFilename) {
		return download(filename)
				.thenCompose(supplier ->
						upload(filename)
								.thenCompose(supplier::streamTo))
				.thenCompose($ -> delete(filename));
	}

	/**
	 * Copies files by a given mapping.
	 *
	 * @param changes mapping from old file names to copy file names
	 * @implNote RemoteFS is considered as an immutable fs, so at first copy will try to create a hard link instead.
	 */
	default Promise<Void> copyBulk(Map<String, String> changes) {
		return Promises.all(changes.entrySet().stream().map(entry -> copy(entry.getKey(), entry.getValue()).toTry()));
	}

	/**
	 * Shortcut for {@link #copyBulk} for a single file.
	 * By default is is equivalent to calling strictCopy(Collections.singletonMap(fileName, newFileName))
	 *
	 * @param filename    file to be moved
	 * @param newFilename new file name
	 */
	default Promise<Void> copy(String filename, String newFilename) {
		return download(filename)
				.thenCompose(supplier ->
						upload(filename)
								.thenCompose(supplier::streamTo));
	}

	/**
	 * Lists files that are matched by glob. Be sure to escape metachars if your filenames contain them
	 *
	 * @param glob specified in {@link java.nio.file.FileSystem#getPathMatcher NIO path matcher} documentation for glob patterns
	 * @return promise for fetched list of file descriptions
	 */
	Promise<List<FileMetadata>> list(String glob);

	/**
	 * Shortcut for {@link #list(String)} with empty 'ping' list request
	 */
	default Promise<Void> ping() {
		return list("").toVoid();
	}

	/**
	 * Shrtcut to get Promise of {@link FileMetadata} of a single file.
	 *
	 * @param filename name of a file to fetch its metadata.
	 * @return promise of file description or <code>null</code>
	 */
	default Promise<FileMetadata> getMetadata(String filename) {
		return list(escapeGlob(filename))
				.thenApply(list -> list.isEmpty() ? null : list.get(0));
	}

	/**
	 * Deletes files that are matched by glob. Be sure to escape metachars if your filenames contain them
	 *
	 * @param glob specified in {@link java.nio.file.FileSystem#getPathMatcher NIO path matcher} documentation for glob patterns
	 * @return marker promise that completes when deletion completes
	 */
	Promise<Void> deleteBulk(String glob);

	/**
	 * Shortcut for {@link #deleteBulk(String)} for a single file.
	 * Given filename is glob-escaped, so only one or zero files could be deleted regardless of given string.
	 *
	 * @param filename name of the file to be deleted
	 * @return marker promise that completes when deletion completes
	 */
	default Promise<Void> delete(String filename) {
		return deleteBulk(escapeGlob(filename));
	}

	static FsClient empty() {
		return EmptyFsClient.INSTANCE;
	}

	default FsClient transform(Function<String, Optional<String>> into, Function<String, Optional<String>> from, Function<String, Optional<String>> globInto) {
		return new TransformFsClient(this, into, from, globInto);
	}

	default FsClient transform(Function<String, Optional<String>> into, Function<String, Optional<String>> from) {
		return new TransformFsClient(this, into, from, $ -> Optional.of("**"));
	}

	// similar to 'chroot'
	default FsClient addingPrefix(String prefix) {
		if (prefix.length() == 0) {
			return this;
		}
		String escapedPrefix = escapeGlob(prefix);
		return transform(
				name -> Optional.of(prefix + name),
				name -> Optional.ofNullable(name.startsWith(prefix) ? name.substring(prefix.length()) : null),
				name -> Optional.of(escapedPrefix + name)
		);
	}

	default FsClient subfolder(String folder) {
		if (folder.length() == 0) {
			return this;
		}
		return addingPrefix(folder.endsWith("/") ? folder : folder + '/');
	}

	default FsClient strippingPrefix(String prefix) {
		if (prefix.length() == 0) {
			return this;
		}
		String escapedPrefix = escapeGlob(prefix);
		return transform(
				name -> Optional.ofNullable(name.startsWith(prefix) ? name.substring(prefix.length()) : null),
				name -> Optional.of(prefix + name),
				name -> Optional.of(name.startsWith(escapedPrefix) ? name.substring(escapedPrefix.length()) : "**")
		);
	}

	default FsClient filter(Predicate<String> predicate) {
		return new FilterFsClient(this, predicate);
	}

	default FsClient mount(String mountpoint, FsClient client) {
		return new MountingFsClient(this, map(mountpoint, client.strippingPrefix(mountpoint + '/')));
	}
}
