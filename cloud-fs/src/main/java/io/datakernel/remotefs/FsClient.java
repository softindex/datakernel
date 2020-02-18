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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.common.exception.StacklessException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.ChannelSuppliers;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.datakernel.common.collection.CollectionUtils.map;
import static io.datakernel.remotefs.RemoteFsUtils.escapeGlob;
import static java.util.stream.Collectors.toList;

/**
 * This interface represents a simple filesystem client with upload, download, move, delete and list operations.
 */
public interface FsClient {
	StacklessException FILE_NOT_FOUND = new StacklessException(FsClient.class, "File not found");
	StacklessException FILE_EXISTS = new StacklessException(FsClient.class, "File already exists");
	StacklessException BAD_PATH = new StacklessException(FsClient.class, "Given file name points to file outside root");
	StacklessException OFFSET_TOO_BIG = new StacklessException(FsClient.class, "Offset exceeds the actual file size");
	StacklessException LENGTH_TOO_BIG = new StacklessException(FsClient.class, "Length with offset exceeds the actual file size");
	StacklessException BAD_RANGE = new StacklessException(FsClient.class, "Given offset or length don't make sense");
	StacklessException MOVING_DIRS = new StacklessException(FsClient.class, "Tried to move, copy delete or replace a directory");
	StacklessException UNSUPPORTED_REVISION = new StacklessException(FsClient.class, "Given revision is not supported");
	StacklessException MALFORMED_GLOB_PATTERN = new StacklessException(FsClient.class, "Malformed glob pattern");

	long DEFAULT_REVISION = 0;

	/**
	 * Returns a consumer of bytebufs which are written (or sent) to the file.
	 * <p>
	 * So, outer promise might fail on connection try, end-of-stream promise
	 * might fail while uploading and result promise might fail when closing.
	 * <p>
	 * If offset is -1 then this will fail when file exists.
	 * If offset is 0 or more then this will override existing file starting from that byte
	 * and fail if file does not exist or is smaller than the offset.
	 * <p>
	 * Note that this method expects that you're uploading the same file prefix with same revision
	 * so 'override' here means 'skip (size - offset) received bytes and append to existing file'.
	 * For real overrides, upload a new file with same name and greater revision.
	 *
	 * @param name   name of the file to upload
	 * @param offset from which byte to write the uploaded data
	 * @return promise for stream consumer of byte buffers
	 */
	Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long offset, long revision);

	// region upload shortcuts

	default Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name, long offset) {
		return upload(name, offset, DEFAULT_REVISION);
	}

	/**
	 * Shortcut for uploading NEW file
	 *
	 * @param name name of the file to upload
	 * @return promise for stream consumer of byte buffers
	 */
	default Promise<ChannelConsumer<ByteBuf>> upload(@NotNull String name) {
		return upload(name, 0);
	}

	default Promise<ChannelConsumer<ByteBuf>> append(@NotNull String name) {
		return getMetadata(name)
				.then(m -> m != null ?
						upload(name, m.isTombstone() ? 0 : m.getSize(), m.getRevision()) :
						upload(name, 0, DEFAULT_REVISION));
	}

	default Promise<Void> truncate(@NotNull String name, long revision) {
		return upload(name, 0, revision)
				.then(consumer -> consumer.accept(null));
	}

	// endregion

	/**
	 * Returns a supplier of bytebufs which are read (or received) from the file.
	 * If file does not exist, or specified range goes beyond it's size,
	 * an error will be returned from the server.
	 * <p>
	 * Length can be set to -1 to download all available data.
	 *
	 * @param name   name of the file to be downloaded
	 * @param offset from which byte to download the file
	 * @param length how much bytes of the file do download
	 * @return promise for stream supplier of byte buffers
	 * @see #download(String, long)
	 * @see #download(String)
	 */
	Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset, long length);

	// region download shortcuts

	/**
	 * Shortcut for downloading the whole file from given offset.
	 *
	 * @return stream supplier of byte buffers
	 * @see #download(String, long, long)
	 * @see #download(String)
	 */
	default Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name, long offset) {
		return download(name, offset, -1);
	}

	/**
	 * Shortcut for downloading the whole available file.
	 *
	 * @param name name of the file to be downloaded
	 * @return stream supplier of byte buffers
	 * @see #download(String, long)
	 * @see #download(String, long, long)
	 */
	default Promise<ChannelSupplier<ByteBuf>> download(@NotNull String name) {
		return download(name, 0, -1);
	}

	// endregion

	/**
	 * Deletes given file.
	 *
	 * @param name name of the file to be deleted
	 * @return marker promise that completes when deletion completes
	 */
	Promise<Void> delete(@NotNull String name, long revision);

	default Promise<Void> delete(@NotNull String name) {
		return delete(name, DEFAULT_REVISION);
	}

	/**
	 * Duplicates a file
	 *
	 * @param name   file to be copied
	 * @param target new file name
	 */
	default Promise<Void> copy(@NotNull String name, @NotNull String target, long targetRevision) {
		return ChannelSuppliers.streamTo(download(name), upload(target, targetRevision));
	}

	default Promise<Void> copy(@NotNull String name, @NotNull String target) {
		return copy(name, target, DEFAULT_REVISION);
	}

	/**
	 * Moves (renames) a file from one name to another.
	 * Equivalent to copying a file to new location and
	 * then deleting the original file.
	 *
	 * @param name   file to be moved
	 * @param target new file name
	 */
	default Promise<Void> move(@NotNull String name, @NotNull String target, long targetRevision, long tombstoneRevision) {
		return copy(name, target, targetRevision)
				.then($ -> delete(name, tombstoneRevision));
	}

	default Promise<Void> move(@NotNull String name, @NotNull String target) {
		return move(name, target, DEFAULT_REVISION, DEFAULT_REVISION);
	}

	default Promise<Void> moveDir(@NotNull String name, @NotNull String target, long targetRevision, long removeRevision) {
		String finalName = name.endsWith("/") ? name : name + '/';
		String finalTarget = target.endsWith("/") ? target : target + '/';
		return list(finalName + "**")
				.then(list -> Promises.all(list.stream()
						.map(meta -> {
							String filename = meta.getName();
							return move(filename, finalTarget + filename.substring(finalName.length()), targetRevision, removeRevision);
						})));
	}

	default Promise<Void> moveDir(@NotNull String name, @NotNull String target) {
		return moveDir(name, target, DEFAULT_REVISION, DEFAULT_REVISION);
	}

	/**
	 * Lists files or their tombstones that are matched by glob.
	 * Be sure to escape metachars if your paths contain them.
	 * <p>
	 * Note that it is not recommended to use this API outside of
	 * Cloud-FS internals or unless you really need recent tombstones for
	 * some kind of merge or repartition operations.
	 * <p>
	 * Use {@link #list} instead.
	 *
	 * @param glob specified in {@link java.nio.file.FileSystem#getPathMatcher NIO path matcher} documentation for glob patterns
	 * @return list of {@link FileMetadata file metadata}
	 */
	Promise<List<FileMetadata>> listEntities(@NotNull String glob);

	/**
	 * Lists files that are matched by glob.
	 * Be sure to escape metachars if your paths contain them.
	 * <p>
	 * This method never returns tombstones.
	 *
	 * @param glob specified in {@link java.nio.file.FileSystem#getPathMatcher NIO path matcher} documentation for glob patterns
	 * @return list of {@link FileMetadata file metadata}
	 */
	default Promise<List<FileMetadata>> list(@NotNull String glob) {
		return listEntities(glob)
				.map(list -> list.stream()
						.filter(m -> !m.isTombstone())
						.collect(toList()));
	}

	/**
	 * Shortcut to get {@link FileMetadata metadata} of a single file or tombstone.
	 *
	 * @param name name of a file to fetch its metadata.
	 * @return promise of file description or <code>null</code>
	 */
	default Promise<@Nullable FileMetadata> getMetadata(@NotNull String name) {
		return listEntities(escapeGlob(name))
				.map(list -> list.isEmpty() ? null : list.get(0));
	}

	/**
	 * Send a ping request.
	 * <p>
	 * Used to check availability of the fs
	 * (is server up in case of remote implementation, for example).
	 */
	default Promise<Void> ping() {
		return listEntities("").toVoid();
	}

	static FsClient zero() {
		return ZeroFsClient.INSTANCE;
	}

	default FsClient transform(@NotNull Function<String, Optional<String>> into, @NotNull Function<String, Optional<String>> from, @NotNull Function<String, Optional<String>> globInto) {
		return new TransformFsClient(this, into, from, globInto);
	}

	default FsClient transform(@NotNull Function<String, Optional<String>> into, @NotNull Function<String, Optional<String>> from) {
		return new TransformFsClient(this, into, from, $ -> Optional.of("**"));
	}

	// similar to 'chroot'
	default FsClient addingPrefix(@NotNull String prefix) {
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

	// similar to 'cd'
	default FsClient subfolder(@NotNull String folder) {
		if (folder.length() == 0) {
			return this;
		}
		return addingPrefix(folder.endsWith("/") ? folder : folder + '/');
	}

	default FsClient strippingPrefix(@NotNull String prefix) {
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

	default FsClient filter(@NotNull Predicate<String> predicate) {
		return new FilterFsClient(this, predicate);
	}

	default FsClient mount(@NotNull String mountpoint, @NotNull FsClient client) {
		return new MountingFsClient(this, map(mountpoint, client.strippingPrefix(mountpoint + '/')));
	}
}
