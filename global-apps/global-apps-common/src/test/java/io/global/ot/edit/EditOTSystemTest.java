package io.global.ot.edit;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.util.List;

import static io.global.ot.edit.DeleteOperation.delete;
import static io.global.ot.edit.InsertOperation.insert;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class EditOTSystemTest {
	private static final OTSystem<EditOperation> SYSTEM = EditOTSystem.createOTSystem();
	private static final String INITIAL_STATE = "abcdefghij";

	// region inserts
	@Test
	public void insertsOnSamePosition() {
		test(insert(2, "123"), insert(2, "123"));
		test(insert(2, "123"), insert(2, "678"));
		test(insert(2, "123"), insert(2, "12345"));
		test(insert(2, "123"), insert(2, "67890"));
		test(insert(2, "123"), insert(2, "67890"));
	}

	@Test
	public void insertsTotallyCross() {
		test(insert(2, "1234"), insert(3, "23"));
		test(insert(2, "1234"), insert(3, "67"));
		test(insert(2, "1234"), insert(2, "12"));
		test(insert(2, "1234"), insert(2, "67"));
		test(insert(2, "1234"), insert(4, "34"));
		test(insert(2, "1234"), insert(4, "67"));
	}

	@Test
	public void insertsPartiallyCross() {
		test(insert(2, "1234"), insert(4, "3456"));
		test(insert(2, "1234"), insert(4, "6789"));
	}

	@Test
	public void insertsDontCross() {
		test(insert(2, "1234"), insert(10, "1234"));
		test(insert(2, "1234"), insert(10, "6789"));
	}

	@Test
	public void insertsSquash() {
		// Will overlap
		testSquash(singletonList(insert(2, "1234567")), insert(2, "4567"), insert(2, "123"));
		testSquash(singletonList(insert(3, "12qw34")), insert(3, "1234"), insert(5, "qw"));
		testSquash(singletonList(insert(3, "12qwerty34")), insert(3, "1234"), insert(5, "qwerty"));
		testSquash(singletonList(insert(3, "123qwerty4")), insert(3, "1234"), insert(6, "qwerty"));
		testSquash(singletonList(insert(3, "1234qwerty")), insert(3, "1234"), insert(7, "qwerty"));

		// Will not overlap
		testSquash(null, insert(1, "123"), insert(5, "qw"));
		testSquash(null, insert(10, "123"), insert(5, "qw"));
	}
	// endregion

	// region deletes
	@Test
	public void deletesOnSamePosition() {
		test(delete(1, "bc"), delete(1, "bc"));
		test(delete(1, "bc"), delete(1, "bcde"));
	}

	@Test
	public void deletesTotallyCross() {
		test(delete(1, "bcdef"), delete(2, "cd"));
		test(delete(1, "bcdef"), delete(2, "cdef"));
	}

	@Test
	public void deletesPartiallyCross() {
		test(delete(1, "bcdef"), delete(3, "defgh"));
	}

	@Test
	public void deletesDontCross() {
		test(delete(1, "bcd"), delete(6, "gh"));
		test(delete(1, "bcd"), delete(4, "ef"));
	}

	@Test
	public void deletesSquash() {
		// Will overlap
		testSquash(singletonList(delete(1, "bcdef")), delete(1, "bcd"), delete(1, "ef"));
		testSquash(singletonList(delete(0, "abcdef")), delete(1, "bcd"), delete(0, "aef"));
		testSquash(singletonList(delete(2, "cdef")), delete(2, "cd"), delete(2, "ef"));

		// Will not overlap
		testSquash(null, delete(1, "bcd"), delete(2, "fg"));
		testSquash(null, delete(2, "cd"), delete(0, "a"));
	}
	// endregion

	// region delete and insert
	@Test
	public void deleteAndInsertOnSamePosition() {
		test(insert(1, "1234"), delete(1, "bcde"));
		test(insert(1, "1234567"), delete(1, "bcde"));
		test(insert(1, "1"), delete(1, "bcde"));
	}

	@Test
	public void deleteAndInsertTotallyCross() {
		test(insert(1, "1234"), delete(2, "cde"));
		test(insert(1, "1234"), delete(2, "c"));
		test(insert(1, "1234"), delete(0, "abcde"));
		test(insert(1, "1234"), delete(0, "abcdefg"));
	}

	@Test
	public void deleteAndInsertPartiallyCross() {
		test(insert(1, "1234"), delete(2, "cdefgh"));
		test(insert(3, "1234"), delete(0, "abcde"));
	}

	@Test
	public void deleteAndInsertDontCross() {
		test(insert(1, "123"), delete(6, "ghij"));
		test(insert(1, "123"), delete(4, "efgh"));
		test(insert(3, "123"), delete(1, "bc"));
		test(insert(5, "123"), delete(1, "bc"));
	}

	@Test
	public void insertAndDeleteSquash() {
		// Totally same
		testSquash(emptyList(), insert(1, "b123c"), delete(1, "b123c"));

		// Delete overlaps insert
		testSquash(singletonList(delete(1, "bc")), insert(2, "123"), delete(1, "b123c"));
		testSquash(singletonList(delete(1, "3c")), insert(1, "b12"), delete(1, "b123c"));
		testSquash(singletonList(delete(1, "b")), insert(2, "123c"), delete(1, "b123c"));

		// Insert overlaps delete
		testSquash(singletonList(insert(2, "15")), insert(2, "12345"), delete(3, "234"));
		testSquash(singletonList(insert(2, "5")), insert(2, "12345"), delete(2, "1234"));
		testSquash(singletonList(insert(2, "1")), insert(2, "12345"), delete(3, "2345"));
	}

	@Test
	public void deleteAndInsertSquash() {
		// Totally same
		testSquash(emptyList(), delete(1, "bcd"), insert(1, "bcd"));

		testSquash(singletonList(insert(2, "123")), delete(2, "cdefg"), insert(2, "123cdefg"));
		testSquash(singletonList(insert(3, "123")), delete(2, "cdefg"), insert(2, "c123defg"));
		testSquash(singletonList(insert(4, "123")), delete(2, "cdefg"), insert(2, "cd123efg"));
		testSquash(singletonList(insert(5, "123")), delete(2, "cdefg"), insert(2, "cde123fg"));
		testSquash(singletonList(insert(6, "123")), delete(2, "cdefg"), insert(2, "cdef123g"));
		testSquash(singletonList(insert(7, "123")), delete(2, "cdefg"), insert(2, "cdefg123"));

		testSquash(singletonList(insert(7, "123efg")), delete(2, "cdefg"), insert(2, "cdefg123efg"));
		testSquash(singletonList(insert(5, "1cde")), delete(2, "cdefg"), insert(2, "cde1cdefg"));
		testSquash(singletonList(insert(5, "123456cde")), delete(2, "cdefg"), insert(2, "cde123456cdefg"));
		testSquash(singletonList(insert(5, "123456e")), delete(2, "cdefg"), insert(2, "cde123456efg"));
		testSquash(null, delete(2, "cdefg"), insert(2, "cde123456g"));
		testSquash(null, delete(2, "cdefg"), insert(2, "c123456efg"));

		testSquash(singletonList(delete(5, "fg")), delete(2, "cdefg"), insert(2, "cde"));
		testSquash(singletonList(delete(3, "defg")), delete(2, "cdefg"), insert(2, "c"));
		testSquash(singletonList(delete(2, "cde")), delete(2, "cdefg"), insert(2, "fg"));
	}
	// endregion

	private void test(EditOperation left, EditOperation right) {
		doTestTransformation(left, right);
		doTestTransformation(right, left);
	}

	private void doTestTransformation(EditOperation left, EditOperation right) {
		try {
			StringBuilder stateLeft = new StringBuilder(INITIAL_STATE);
			StringBuilder stateRight = new StringBuilder(INITIAL_STATE);
			TransformResult<EditOperation> result = SYSTEM.transform(left, right);

			checkIfValid(left, stateLeft);
			left.apply(stateLeft);
			result.left.forEach(editorOperation -> {
				checkIfValid(editorOperation, stateLeft);
				editorOperation.apply(stateLeft);
			});

			checkIfValid(right, stateRight);
			right.apply(stateRight);
			result.right.forEach(editorOperation -> {
				checkIfValid(editorOperation, stateRight);
				editorOperation.apply(stateRight);
			});

			assertEquals(stateLeft.toString(), stateRight.toString());
		} catch (OTTransformException e) {
			throw new AssertionError(e);
		}
	}

	private void testSquash(@Nullable List<EditOperation> expectedSquash, EditOperation first, EditOperation second) {
		List<EditOperation> ops = asList(first, second);
		List<EditOperation> actualSquash = SYSTEM.squash(ops);
		if (expectedSquash == null) {
			assertEquals(ops, actualSquash);
		} else {
			assertEquals(expectedSquash, actualSquash);
		}
	}

	private void checkIfValid(EditOperation operation, StringBuilder builder) {
		if (operation instanceof DeleteOperation) {
			int position = operation.getPosition();
			String content = operation.getContent();
			if (!content.equals(builder.substring(position, position + content.length()))) {
				throw new IllegalStateException("Trying to delete non-present content");
			}
		}
	}
}
