package io.global.ot.map;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.util.CollectionUtils.map;
import static io.global.ot.map.SetValue.set;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public final class MapOTSystemTest {
	private final OTSystem<MapOperation<String, String>> SYSTEM = MapOTSystem.createOTSystem(String::compareTo);

	@Test
	public void testIsEmpty() {
		assertTrue(SYSTEM.isEmpty(MapOperation.of(map())));
		assertTrue(SYSTEM.isEmpty(MapOperation.of(map("a", set(null, null)))));
		assertTrue(SYSTEM.isEmpty(MapOperation.of(map("a", set("a", "a")))));
		assertFalse(SYSTEM.isEmpty(MapOperation.of(map("a", set(null, "")))));
	}

	@Test
	public void testInversion() {
		MapOTState<String, String> state = new MapOTState<>();
		MapOperation<String, String> initialOp = MapOperation.of(map(
				"a", set(null, "val_a"),
				"b", set(null, "val_b"),
				"c", set(null, "val_c")
		));
		state.apply(initialOp);
		Map<String, String> stateMap = state.getMap();
		Map<String, String> initialMap = new HashMap<>(stateMap);
		List<MapOperation<String, String>> ops = asList(
				MapOperation.of(map(
						"a", set("val_a", "new_val_a"),
						"b", set("val_b", "val_bb"),
						"c", set("val_c", "new_val_c"))),
				MapOperation.of(map(
						"c", set("new_val_c", "new_val_cc"),
						"d", set(null, "val_d")))
		);
		ops.forEach(state::apply);
		assertNotEquals(initialMap, stateMap);
		List<MapOperation<String, String>> inverted = SYSTEM.invert(ops);
		inverted.forEach(state::apply);
		assertEquals(initialMap, stateMap);
	}

	@Test
	public void testTransform() throws OTTransformException {
		MapOTState<String, String> stateLeft = new MapOTState<>();
		MapOTState<String, String> stateRight = new MapOTState<>();

		MapOperation<String, String> left = MapOperation.of(map(
				"a", set(null, "ab"),
				"b", set("b", "cd"),
				"c", set(null, "ef")
		));
		stateLeft.apply(left);
		MapOperation<String, String> right = MapOperation.of(map(
				"a", set(null, "bb"),
				"b", set("b", null),
				"c", set(null, "ef")
		));
		stateRight.apply(right);

		TransformResult<MapOperation<String, String>> transform = SYSTEM.transform(left, right);
		transform.left.forEach(stateLeft::apply);
		transform.right.forEach(stateRight::apply);
		assertEquals(stateLeft.getMap(), stateRight.getMap());

		Map<String, String> expected = map(
				"a", "bb",
				"b", "cd",
				"c", "ef"
		);
		assertEquals(expected, stateLeft.getMap());
	}

	@Test
	public void testSquash() {
		List<MapOperation<String, String>> ops = asList(
				MapOperation.of(map(
						"a", set(null, "val_a"),
						"b", set(null, "val_b"),
						"c", set(null, "val_c")
				)),
				MapOperation.of(map(
						"a", set("val_a", "new_val_a"),
						"b", set("val_b", null),
						"c", set("val_c", "new_val_c")
				)),
				MapOperation.of(map(
						"c", set("new_val_c", "new_val_cc"),
						"d", set(null, "val_d")
				))
		);

		assertEquals(singletonList(MapOperation.of(map(
				"a", set(null, "new_val_a"),
				"c", set(null, "new_val_cc"),
				"d", set(null, "val_d")
		))), SYSTEM.squash(ops));
	}

}
