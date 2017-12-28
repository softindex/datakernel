package io.datakernel.logfs.ot;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.datakernel.logfs.LogFile;
import io.datakernel.logfs.LogPosition;
import io.datakernel.utils.GsonAdapters;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.utils.GsonAdapters.oneline;

public final class LogDiffJson<D> extends TypeAdapter<LogDiff<D>> {
	public static final String POSITIONS = "positions";
	public static final String LOG = "log";
	public static final String FROM = "from";
	public static final String TO = "to";
	public static final String OPS = "ops";

	public final static TypeAdapter<LogPosition> LOG_POSITION_JSON = oneline(new LogPositionJson());

	public final static class LogPositionJson extends TypeAdapter<LogPosition> {
		@Override
		public void write(JsonWriter out, LogPosition value) throws IOException {
			out.beginArray();
			out.value(value.getLogFile().getName());
			out.value(value.getLogFile().getN());
			out.value(value.getPosition());
			out.endArray();
		}

		@Override
		public LogPosition read(JsonReader in) throws IOException {
			in.beginArray();
			String name = in.nextString();
			int n = in.nextInt();
			long position = in.nextLong();
			in.endArray();
			return LogPosition.create(new LogFile(name, n), position);
		}
	}

	private final TypeAdapter<List<D>> opsAdapter;

	private LogDiffJson(TypeAdapter<List<D>> opsAdapter) {
		this.opsAdapter = opsAdapter;
	}

	public static <D> LogDiffJson<D> create(TypeAdapter<D> opAdapter) {
		return new LogDiffJson<D>(GsonAdapters.ofList(opAdapter));
	}

	@Override
	public void write(JsonWriter out, LogDiff<D> multilogDiff) throws IOException {
		out.beginObject();
		out.name(POSITIONS);
		out.beginArray();
		for (Map.Entry<String, LogDiff.LogPositionDiff> entry : multilogDiff.positions.entrySet()) {
			out.beginObject();
			out.name(LOG);
			out.value(entry.getKey());
			out.name(FROM);
			LOG_POSITION_JSON.write(out, entry.getValue().from);
			out.name(TO);
			LOG_POSITION_JSON.write(out, entry.getValue().to);
			out.endObject();
		}
		out.endArray();
		out.name(OPS);
		opsAdapter.write(out, multilogDiff.diffs);
		out.endObject();
	}

	@Override
	public LogDiff<D> read(JsonReader in) throws IOException {
		in.beginObject();
		checkArgument(POSITIONS.equals(in.nextName()));
		in.beginArray();
		Map<String, LogDiff.LogPositionDiff> positions = new LinkedHashMap<>();
		while (in.hasNext()) {
			in.beginObject();
			checkArgument(LOG.equals(in.nextName()));
			String log = in.nextString();
			checkArgument(FROM.equals(in.nextName()));
			LogPosition from = LOG_POSITION_JSON.read(in);
			checkArgument(TO.equals(in.nextName()));
			LogPosition to = LOG_POSITION_JSON.read(in);
			positions.put(log, new LogDiff.LogPositionDiff(from, to));
			in.endObject();
		}
		in.endArray();
		checkArgument(OPS.equals(in.nextName()));
		List<D> ops = opsAdapter.read(in);
		in.endObject();
		return LogDiff.of(positions, ops);
	}

}
