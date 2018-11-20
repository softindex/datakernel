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

package io.datakernel.logfs.ot;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.codec.StructuredCodecs;
import io.datakernel.codec.StructuredInput;
import io.datakernel.codec.StructuredOutput;
import io.datakernel.exception.ParseException;
import io.datakernel.logfs.LogFile;
import io.datakernel.logfs.LogPosition;
import io.datakernel.logfs.ot.LogDiff.LogPositionDiff;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.codec.StructuredCodecs.oneline;

public final class LogDiffCodec<D> implements StructuredCodec<LogDiff<D>> {
	public static final String POSITIONS = "positions";
	public static final String LOG = "log";
	public static final String FROM = "from";
	public static final String TO = "to";
	public static final String OPS = "ops";

	public final static StructuredCodec<LogPosition> LOG_POSITION_CODEC = oneline(new LogPositionCodec());

	public final static class LogPositionCodec implements StructuredCodec<LogPosition> {
		@Override
		public void encode(StructuredOutput out, LogPosition value) {
			out.writeTuple(() -> {
				out.writeString(value.getLogFile().getName());
				out.writeInt(value.getLogFile().getN());
				out.writeLong(value.getPosition());
			});
		}

		@Override
		public LogPosition decode(StructuredInput in) throws ParseException {
			return in.readTuple($ -> {
				String name = in.readString();
				int n = in.readInt();
				long position = in.readLong();
				return LogPosition.create(new LogFile(name, n), position);
			});
		}
	}

	private final StructuredCodec<List<D>> opsCodec;

	private LogDiffCodec(StructuredCodec<List<D>> opsCodec) {
		this.opsCodec = opsCodec;
	}

	public static <D> LogDiffCodec<D> create(StructuredCodec<D> opAdapter) {
		return new LogDiffCodec<>(StructuredCodecs.ofList(opAdapter));
	}

	@Override
	public void encode(StructuredOutput out, LogDiff<D> multilogDiff) {
		out.writeObject(() -> {
			out.writeKey(POSITIONS);
			out.writeTuple(() -> {
				for (Map.Entry<String, LogPositionDiff> entry : multilogDiff.getPositions().entrySet()) {
					out.writeObject(() -> {
						out.writeKey(LOG);
						out.writeString(entry.getKey());
						out.writeKey(FROM);
						LOG_POSITION_CODEC.encode(out, entry.getValue().from);
						out.writeKey(TO);
						LOG_POSITION_CODEC.encode(out, entry.getValue().to);
					});
				}
			});
			out.writeKey(OPS);
			opsCodec.encode(out, multilogDiff.getDiffs());
		});
	}

	@Override
	public LogDiff<D> decode(StructuredInput in) throws ParseException {
		return in.readObject($ -> {
			Map<String, LogPositionDiff> positions = new LinkedHashMap<>();
			in.readKey(POSITIONS);
			in.readTuple(() -> {
				while (in.hasNext()) {
					in.readObject(() -> {
						in.readKey(LOG);
						String log = in.readString();
						in.readKey(FROM);
						LogPosition from = LOG_POSITION_CODEC.decode(in);
						in.readKey(TO);
						LogPosition to = LOG_POSITION_CODEC.decode(in);
						positions.put(log, new LogPositionDiff(from, to));
					});
				}
			});
			in.readKey(OPS);
			List<D> ops = opsCodec.decode(in);
			return LogDiff.of(positions, ops);
		});
	}

}
