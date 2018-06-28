package io.datakernel.cube;

import io.datakernel.cube.ot.CubeDiff;
import io.datakernel.logfs.ot.LogDiff;

import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public interface CubeDiffScheme<D> {
	D wrap(CubeDiff cubeDiff);

	default List<CubeDiff> unwrap(D diff) {
		return unwrapToStream(diff).collect(toList());
	}

	default Stream<CubeDiff> unwrapToStream(D diff) {
		return unwrap(diff).stream();
	}

	static CubeDiffScheme<LogDiff<CubeDiff>> ofLogDiffs() {
		return new CubeDiffScheme<LogDiff<CubeDiff>>() {
			@Override
			public LogDiff<CubeDiff> wrap(CubeDiff cubeDiff) {
				return LogDiff.forCurrentPosition(cubeDiff);
			}

			@Override
			public List<CubeDiff> unwrap(LogDiff<CubeDiff> diff) {
				return diff.getDiffs();
			}
		};
	}

	static CubeDiffScheme<CubeDiff> ofCubeDiffs() {
		return new CubeDiffScheme<CubeDiff>() {
			@Override
			public CubeDiff wrap(CubeDiff cubeDiff) {
				return cubeDiff;
			}

			@Override
			public List<CubeDiff> unwrap(CubeDiff diff) {
				return singletonList(diff);
			}
		};
	}
}
