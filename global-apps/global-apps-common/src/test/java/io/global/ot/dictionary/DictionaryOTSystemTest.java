package io.global.ot.dictionary;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.util.CollectionUtils.map;
import static io.global.ot.dictionary.SetOperation.set;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public final class DictionaryOTSystemTest {
	private final OTSystem<DictionaryOperation> SYSTEM = DictionaryOTSystem.createOTSystem();

	@Test
	public void testIsEmpty() {
		assertTrue(SYSTEM.isEmpty(DictionaryOperation.of(map())));
		assertTrue(SYSTEM.isEmpty(DictionaryOperation.of(map("a", set(null, null)))));
		assertTrue(SYSTEM.isEmpty(DictionaryOperation.of(map("a", set("a", "a")))));
		assertFalse(SYSTEM.isEmpty(DictionaryOperation.of(map("a", set(null, "")))));
	}

	@Test
	public void testInversion() {
		DictionaryOTState state = new DictionaryOTState();
		DictionaryOperation initialOp = DictionaryOperation.of(map(
				"a", set(null, "val_a"),
				"b", set(null, "val_b"),
				"c", set(null, "val_c")
		));
		state.apply(initialOp);
		Map<String, String> dictionary = state.getDictionary();
		Map<String, String> initialDictionary = new HashMap<>(dictionary);
		List<DictionaryOperation> ops = asList(
				DictionaryOperation.of(map(
						"a", set("val_a", "new_val_a"),
						"b", set("val_b", "val_bb"),
						"c", set("val_c", "new_val_c"))),
				DictionaryOperation.of(map(
						"c", set("new_val_c", "new_val_cc"),
						"d", set(null, "val_d")))
		);
		ops.forEach(state::apply);
		assertNotEquals(initialDictionary, dictionary);
		List<DictionaryOperation> inverted = SYSTEM.invert(ops);
		inverted.forEach(state::apply);
		assertEquals(initialDictionary, dictionary);
	}

	@Test
	public void testTransform() throws OTTransformException {
		DictionaryOTState stateLeft = new DictionaryOTState();
		DictionaryOTState stateRight = new DictionaryOTState();

		DictionaryOperation left = DictionaryOperation.of(map(
				"a", set(null, "ab"),
				"b", set("b", "cd"),
				"c", set(null, "ef")
		));
		stateLeft.apply(left);
		DictionaryOperation right = DictionaryOperation.of(map(
				"a", set(null, "bb"),
				"b", set("b", null),
				"c", set(null, "ef")
		));
		stateRight.apply(right);

		TransformResult<DictionaryOperation> transform = SYSTEM.transform(left, right);
		transform.left.forEach(stateLeft::apply);
		transform.right.forEach(stateRight::apply);
		assertEquals(stateLeft.getDictionary(), stateRight.getDictionary());

		Map<Object, Object> expected = map(
				"a", "bb",
				"b", "cd",
				"c", "ef"
		);
		assertEquals(expected, stateLeft.getDictionary());
	}

	@Test
	public void testSquash() {
		List<DictionaryOperation> ops = asList(
				DictionaryOperation.of(map(
						"a", set(null, "val_a"),
						"b", set(null, "val_b"),
						"c", set(null, "val_c")
				)),
				DictionaryOperation.of(map(
						"a", set("val_a", "new_val_a"),
						"b", set("val_b", null),
						"c", set("val_c", "new_val_c")
				)),
				DictionaryOperation.of(map(
						"c", set("new_val_c", "new_val_cc"),
						"d", set(null, "val_d")
				))
		);

		assertEquals(singletonList(DictionaryOperation.of(map(
				"a", set(null, "new_val_a"),
				"c", set(null, "new_val_cc"),
				"d", set(null, "val_d")
		))), SYSTEM.squash(ops));
	}

}
