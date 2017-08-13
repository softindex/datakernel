package io.datakernel.logfs.ot;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import io.datakernel.ot.DiffPair;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.util.Preconditions;

import java.util.*;

import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.intersection;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class LogOT {
	public static <T> OTSystem<LogDiff<T>> createLogOT(final OTSystem<T> otSystem) {
		return OTSystemImpl.<LogDiff<T>>create()
				.withTransformFunction(LogDiff.class, LogDiff.class, new OTSystemImpl.TransformFunction<LogDiff<T>, LogDiff<T>, LogDiff<T>>() {
					@Override
					public DiffPair<? extends LogDiff<T>> transform(LogDiff<T> left, LogDiff<T> right) {
						Set<String> intersection = intersection(left.positions.keySet(), right.positions.keySet());
						if (intersection.isEmpty()) {
							DiffPair<T> transformed = otSystem.transform(DiffPair.of(left.diffs, right.diffs));
							return DiffPair.of(
									LogDiff.of(right.positions, transformed.left),
									LogDiff.of(left.positions, transformed.right));
						}

						int comparison = 0;
						for (String log : intersection) {
							LogDiff.LogPositionDiff leftPosition = left.positions.get(log);
							LogDiff.LogPositionDiff rightPosition = right.positions.get(log);
							Preconditions.check(leftPosition.from == rightPosition.from);
							comparison += leftPosition.compareTo(rightPosition);
						}

						if (comparison > 0) {
							return DiffPair.right(asList(inverse(otSystem, right), left));
						} else {
							return DiffPair.left(asList(inverse(otSystem, left), right));
						}
					}
				})
				.withEmptyPredicate(LogDiff.class, new OTSystemImpl.EmptyPredicate<LogDiff<T>>() {
					@Override
					public boolean isEmpty(LogDiff<T> commit) {
						return commit.positions.isEmpty() && all(commit.diffs, new Predicate<T>() {
							@Override
							public boolean apply(T op) {
								return otSystem.isEmpty(op);
							}
						});
					}
				})
				.withSquashFunction(LogDiff.class, LogDiff.class, new OTSystemImpl.SquashFunction<LogDiff<T>, LogDiff<T>, LogDiff<T>>() {
					@Override
					public LogDiff<T> trySquash(LogDiff<T> commit1, LogDiff<T> commit2) {
						Map<String, LogDiff.LogPositionDiff> positions = new HashMap<>(commit1.positions);
						positions.putAll(commit2.positions);
						List<T> ops = new ArrayList<>(commit1.diffs.size() + commit2.diffs.size());
						ops.addAll(commit1.diffs);
						ops.addAll(commit2.diffs);
						return LogDiff.of(positions, otSystem.squash(ops));
					}
				})
				.withInvertFunction(LogDiff.class, new OTSystemImpl.InvertFunction<LogDiff<T>>() {
					@Override
					public List<? extends LogDiff<T>> invert(LogDiff<T> commit) {
						return singletonList(LogDiff.of(
								transformValues(commit.positions, new Function<LogDiff.LogPositionDiff, LogDiff.LogPositionDiff>() {
									@Override
									public LogDiff.LogPositionDiff apply(LogDiff.LogPositionDiff position) {
										return position.inverse();
									}
								}),
								otSystem.invert(commit.diffs)));
					}
				})
				;

	}

	static <T> LogDiff<T> inverse(OTSystem<T> otSystem, LogDiff<T> commit) {
		return LogDiff.of(
				transformValues(commit.positions, new Function<LogDiff.LogPositionDiff, LogDiff.LogPositionDiff>() {
					@Override
					public LogDiff.LogPositionDiff apply(LogDiff.LogPositionDiff position) {
						return position.inverse();
					}
				}),
				otSystem.invert(commit.diffs));
	}
}
