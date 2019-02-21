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

package io.global.fs.util;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codec.registry.CodecFactory;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FileMetadata;
import io.global.common.Hash;
import io.global.fs.api.GlobalFsCheckpoint;

import static io.datakernel.codec.StructuredCodecs.tuple;
import static io.global.common.BinaryDataFormats.createGlobal;

public final class BinaryDataFormats {
	private BinaryDataFormats() {
		throw new AssertionError("nope.");
	}

	public static final CodecFactory REGISTRY = createGlobal()
			.with(FileMetadata.class, registry ->
					tuple(FileMetadata::parse,
							FileMetadata::getName, registry.get(String.class),
							FileMetadata::getSize, registry.get(long.class),
							FileMetadata::getTimestamp, registry.get(long.class),
							FileMetadata::getRevision, registry.get(long.class)))

			.with(GlobalFsCheckpoint.class, registry ->
					tuple(GlobalFsCheckpoint::parse,
							GlobalFsCheckpoint::getFilename, registry.get(String.class),
							GlobalFsCheckpoint::getPosition, registry.get(long.class),
							GlobalFsCheckpoint::getDigestState, registry.get(byte[].class),
							GlobalFsCheckpoint::getSimKeyHash, registry.get(Hash.class).nullable()));

	public static ByteBuf readBuf(ByteBuf buf) throws ParseException {
		int size;
		try {
			size = buf.readVarInt();
		} catch (Exception e) {
			throw new ParseException(e);
		}
		if (size <= 0 || size > buf.readRemaining()) throw new ParseException("Invalid chunk size");
		ByteBuf slice = buf.slice(size);
		buf.moveHead(size);
		return slice;
	}
}
