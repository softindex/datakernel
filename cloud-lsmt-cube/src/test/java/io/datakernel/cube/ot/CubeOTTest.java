package io.datakernel.cube.ot;

import io.datakernel.aggregation.AggregationChunk;
import io.datakernel.aggregation.PrimaryKey;
import io.datakernel.aggregation.ot.AggregationDiff;
import io.datakernel.etl.LogDiff;
import io.datakernel.etl.LogOT;
import io.datakernel.etl.LogPositionDiff;
import io.datakernel.multilog.LogFile;
import io.datakernel.multilog.LogPosition;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.TransformResult.ConflictResolution;
import io.datakernel.ot.exceptions.OTTransformException;
import io.datakernel.ot.system.OTSystem;
import org.hamcrest.collection.IsEmptyCollection;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static io.datakernel.aggregation.PrimaryKey.ofArray;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;

public class CubeOTTest {
	private OTSystem<LogDiff<CubeDiff>> logSystem;

	@Before
	public void before() {
		logSystem = LogOT.createLogOT(CubeOT.createCubeOT());
	}

	private static LogPositionDiff positionDiff(LogFile logFile, long start, long end) {
		return new LogPositionDiff(LogPosition.create(logFile, start), LogPosition.create(logFile, end));
	}

	private static CubeDiff cubeDiff(String key, AggregationChunk... added) {
		return cubeDiff(key, asList(added), Collections.emptyList());
	}

	private static CubeDiff cubeDiff(String key, List<AggregationChunk> added, List<AggregationChunk> removed) {
		return CubeDiff.of(singletonMap(key, AggregationDiff.of(new HashSet<>(added),
				new HashSet<>(removed))));
	}

	private static AggregationChunk chunk(long chunkId, List<String> fields, PrimaryKey minKey, PrimaryKey maxKey, int count) {
		return AggregationChunk.create(chunkId, fields, minKey, maxKey, count);
	}

	private static List<AggregationChunk> addedChunks(Collection<CubeDiff> cubeDiffs) {
		return cubeDiffs.stream()
				.flatMap(cubeDiff -> cubeDiff.keySet().stream().map(cubeDiff::get))
				.map(AggregationDiff::getAddedChunks)
				.flatMap(Collection::stream)
				.collect(toList());
	}

	@Test
	public void test() throws OTTransformException {
		LogFile logFile = new LogFile("file", 1);
		List<String> fields = asList("field1", "field2");
		LogDiff<CubeDiff> changesLeft = LogDiff.of(
				singletonMap("clicks", positionDiff(logFile, 0, 10)),
				cubeDiff("key", chunk(1, fields, ofArray("str", 10), ofArray("str", 20), 15)));

		LogDiff<CubeDiff> changesRight = LogDiff.of(
				singletonMap("clicks", positionDiff(logFile, 0, 20)),
				cubeDiff("key", chunk(1, fields, ofArray("str", 10), ofArray("str", 25), 30)));
		TransformResult<LogDiff<CubeDiff>> transform = logSystem.transform(changesLeft, changesRight);

		assertTrue(transform.hasConflict());
		assertEquals(ConflictResolution.RIGHT, transform.resolution);
		assertThat(transform.right, IsEmptyCollection.empty());

		LogDiff<CubeDiff> result = LogDiff.of(
				singletonMap("clicks", positionDiff(logFile, 10, 20)),
				cubeDiff("key", addedChunks(changesRight.getDiffs()), addedChunks(changesLeft.getDiffs())));

		assertEquals(1, transform.left.size());
		assertEquals(result, transform.left.get(0));
	}

}
