/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalfs.api;

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.global.common.SignedData;

import static io.datakernel.util.Preconditions.checkState;

public final class DataFrame {
	@Nullable
	private final ByteBuf buf;

	@Nullable
	private final SignedData<GlobalFsCheckpoint> checkpoint;

	// region creators
	private DataFrame(@Nullable ByteBuf buf, @Nullable SignedData<GlobalFsCheckpoint> checkpoint) {
		assert buf != null ^ checkpoint != null;
		this.buf = buf;
		this.checkpoint = checkpoint;
	}

	public static DataFrame of(ByteBuf buf) {
		return new DataFrame(buf, null);
	}

	public static DataFrame of(SignedData<GlobalFsCheckpoint> checkpoint) {
		return new DataFrame(null, checkpoint);
	}
	// endregion

	public boolean isBuf() {
		return buf != null;
	}

	public boolean isCheckpoint() {
		return checkpoint != null;
	}

	public DataFrame slice() {
		if (buf == null) {
			return this;
		}
		return new DataFrame(buf.slice(), null);
	}

	public void recycle() {
		if (buf != null) {
			buf.recycle();
		}
	}

	public ByteBuf getBuf() {
		checkState(isBuf());
		assert buf != null;
		return buf;
	}

	public SignedData<GlobalFsCheckpoint> getCheckpoint() {
		checkState(isCheckpoint());
		assert checkpoint != null;
		return checkpoint;
	}

	@Override
	public String toString() {
		assert !isBuf() || buf != null;
		return "DataFrame{" + (isBuf() ? "buf=(size=" + buf.readRemaining() + ")" : "checkpoint=" + checkpoint) + '}';
	}
}
