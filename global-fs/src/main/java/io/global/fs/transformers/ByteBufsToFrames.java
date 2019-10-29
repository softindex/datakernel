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

package io.global.fs.transformers;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.csp.process.AbstractChannelTransformer;
import io.datakernel.promise.Promise;
import io.global.fs.api.DataFrame;

/**
 * Abstracted out bytebuf cutting code
 */
abstract class ByteBufsToFrames extends AbstractChannelTransformer<ByteBufsToFrames, ByteBuf, DataFrame> {
	protected long position;
	protected long nextCheckpoint;

	ByteBufsToFrames(long offset) {
		position = nextCheckpoint = offset;
	}

	protected abstract Promise<Void> postCheckpoint();

	protected Promise<Void> postByteBuf(ByteBuf buf) {
		position += buf.readRemaining();
		return send(DataFrame.of(buf));
	}

	@Override
	protected Promise<Void> onProcessStart() {
		return postCheckpoint();
	}

	@Override
	protected Promise<Void> onItem(ByteBuf item) {
		int size = item.readRemaining();

		if (position + size < nextCheckpoint) {
			return postByteBuf(item);
		}

		int bytesUntilCheckpoint = (int) (nextCheckpoint - position);
		int remaining = size - bytesUntilCheckpoint;

		if (remaining == 0) {
			return postByteBuf(item)
					.then($ -> postCheckpoint());
		}

		ByteBuf until = item.slice(bytesUntilCheckpoint);
		item.moveHead(bytesUntilCheckpoint);
		return postByteBuf(until)
				.then($ -> postCheckpoint())
				.then($ -> onItem(item));
	}
}
