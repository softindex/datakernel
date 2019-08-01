package io.global.documents.document.edit;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.OTSystemImpl.TransformFunction;
import io.datakernel.ot.TransformResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.global.documents.document.edit.DeleteOperation.delete;
import static io.global.documents.document.edit.InsertOperation.insert;
import static java.util.Collections.singletonList;

public class EditOTSystem {
	private EditOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<EditOperation> createOTSystem() {
		return OTSystemImpl.<EditOperation>create()
				.withTransformFunction(InsertOperation.class, InsertOperation.class, transformInsertAndInsert())
				.withTransformFunction(DeleteOperation.class, DeleteOperation.class, transformDeleteAndDelete())
				.withTransformFunction(DeleteOperation.class, InsertOperation.class, transformDeleteAndInsert())

				.withEmptyPredicate(InsertOperation.class, op -> op.getContent().isEmpty())
				.withEmptyPredicate(DeleteOperation.class, op -> op.getContent().isEmpty())

				.withInvertFunction(InsertOperation.class, op -> singletonList(op.invert()))
				.withInvertFunction(DeleteOperation.class, op -> singletonList(op.invert()))

				.withSquashFunction(InsertOperation.class, InsertOperation.class, EditOTSystem::squashInsertAndInsert)
				.withSquashFunction(DeleteOperation.class, DeleteOperation.class, EditOTSystem::squashDeleteAndDelete)
				.withSquashFunction(DeleteOperation.class, InsertOperation.class, EditOTSystem::squashDeleteAndInsert)
				.withSquashFunction(InsertOperation.class, DeleteOperation.class, EditOTSystem::squashInsertAndDelete);
	}

	@NotNull
	private static TransformFunction<EditOperation, InsertOperation, InsertOperation> transformInsertAndInsert() {
		return (left, right) -> {
			if (left.getPosition() == right.getPosition() && left.getContent().equals(right.getContent())) {
				return TransformResult.empty();
			}

			if (left.getPosition() == right.getPosition() &&
					left.getContent().compareTo(right.getContent()) < 0 ||
					left.getPosition() < right.getPosition()) {
				return TransformResult.of(insert(right.getPosition() + left.getLength(), right.getContent()), left);
			}

			return TransformResult.of(right, insert(left.getPosition() + right.getLength(), left.getContent()));
		};
	}

	@NotNull
	private static TransformFunction<EditOperation, DeleteOperation, DeleteOperation> transformDeleteAndDelete() {
		return (left, right) -> {
			// Left operation always has lesser position
			if (left.getPosition() > right.getPosition()) {
				TransformResult<EditOperation> result = doTransformDeleteAndDelete(right, left);
				return TransformResult.of(result.right, result.left);
			} else {
				return doTransformDeleteAndDelete(left, right);
			}
		};
	}

	@NotNull
	private static TransformFunction<EditOperation, DeleteOperation, InsertOperation> transformDeleteAndInsert() {
		return (left, right) -> {
			if (left.getPosition() == right.getPosition()) {
				return TransformResult.of(right, delete(right.getPosition() + right.getLength(), left.getContent().substring(0, left.getLength())));
			}

			if (right.getPosition() > left.getPosition() && left.getPosition() + left.getLength() > right.getPosition()) {
				int index = right.getPosition() - left.getPosition();
				return TransformResult.right(delete(left.getPosition(),
						left.getContent().substring(0, index) +
								right.getContent() + left.getContent().substring(index)));
			}

			if (left.getPosition() > right.getPosition()) {
				return TransformResult.of(right, delete(left.getPosition() + right.getLength(), left.getContent()));
			}

			return TransformResult.of(insert(right.getPosition() - left.getLength(), right.getContent()), left);
		};
	}

	private static TransformResult<EditOperation> doTransformDeleteAndDelete(DeleteOperation left, DeleteOperation right) {
		if (left.equals(right)) {
			return TransformResult.empty();
		}

		if (left.getPosition() == right.getPosition()) {
			if (left.getLength() > right.getLength()) {
				return TransformResult.right(delete(left.getPosition(),
						left.getContent().substring(left.getLength() - right.getLength())));
			} else {
				return TransformResult.left(delete(left.getPosition(),
						right.getContent().substring(right.getLength() - left.getLength())));
			}
		}

		if (left.getPosition() + left.getLength() > right.getPosition()) {
			if (left.getPosition() + left.getLength() >= right.getPosition() + right.getLength()) {

				// Total crossing
				return TransformResult.right(delete(left.getPosition(),
						left.getContent().substring(0, right.getPosition() - left.getPosition()) +
								left.getContent().substring(right.getPosition() - left.getPosition() + right.getLength())));
			}

			// Partial crossing
			return TransformResult.of(
					delete(left.getPosition(), right.getContent().substring(left.getPosition() + left.getLength() - right.getPosition())),
					delete(left.getPosition(), left.getContent().substring(0, right.getPosition() - left.getPosition()))
			);
		}

		// No crossing
		return TransformResult.of(delete(right.getPosition() - left.getLength(), right.getContent()), left);
	}

	@Nullable
	private static EditOperation squashInsertAndInsert(InsertOperation first, InsertOperation second) {
		if (first.getPosition() <= second.getPosition() && first.getPosition() + first.getLength() >= second.getPosition()) {
			return insert(first.getPosition(),
					first.getContent().substring(0, second.getPosition() - first.getPosition()) +
							second.getContent() +
							first.getContent().substring(second.getPosition() - first.getPosition()));
		} else {
			return null;
		}
	}

	@Nullable
	private static EditOperation squashDeleteAndDelete(DeleteOperation first, DeleteOperation second) {
		// Second delete should overlap with first's position
		if (second.getPosition() <= first.getPosition() && second.getPosition() + second.getLength() >= first.getPosition()) {
			return delete(second.getPosition(),
					second.getContent().substring(0, first.getPosition() - second.getPosition()) +
							first.getContent() +
							second.getContent().substring(first.getPosition() - second.getPosition()));
		} else {
			return null;
		}
	}

	@Nullable
	private static EditOperation squashInsertAndDelete(InsertOperation first, DeleteOperation second) {
		// Operations cancel each other
		if (second.getPosition() == first.getPosition() && second.getContent().equals(first.getContent())) {
			// Empty operation
			return insert(0, "");
		}

		// Delete totally overlaps insert
		if (second.getPosition() <= first.getPosition() &&
				second.getPosition() + second.getLength() >= first.getPosition() &&
				second.getPosition() + second.getLength() >= first.getPosition() + first.getLength()) {
			return getDelete(first, second);
		}

		// Insert totally overlaps delete
		if (first.getPosition() <= second.getPosition() &&
				first.getPosition() + first.getLength() >= second.getPosition() &&
				first.getPosition() + first.getLength() >= second.getPosition() + second.getLength()) {
			return getDelete(second, first).invert();
		}

		return null;
	}

	@Nullable
	private static EditOperation squashDeleteAndInsert(DeleteOperation first, InsertOperation second) {
		// if positions match
		if (first.getPosition() == second.getPosition()) {
			if (second.getLength() <= first.getLength()) {
				if (first.getContent().startsWith(second.getContent())) {
					return delete(first.getPosition() + second.getLength(),
							first.getContent().substring(second.getLength()));
				}
				if (first.getContent().endsWith(second.getContent())) {
					return delete(first.getPosition(),
							first.getContent().substring(0, first.getLength() - second.getLength()));
				}

			} else {
				return getInsert(first, second);
			}
		}

		return null;
	}

	@NotNull
	private static DeleteOperation getDelete(EditOperation first, EditOperation second) {
		return delete(second.getPosition(),
				second.getContent().substring(0, first.getPosition() - second.getPosition()) +
						second.getContent().substring(first.getPosition() - second.getPosition() + first.getLength()));
	}

	@Nullable
	private static EditOperation getInsert(EditOperation first, EditOperation second) {
		assert second.getLength() > first.getLength();
		assert first.getPosition() == second.getPosition();

		String content = second.getContent();
		String subContent = first.getContent();
		int index = first.getPosition();

		int startIndex = getStartIndex(subContent, content);
		int endIndex = getEndIndex(subContent, content);

		if (startIndex == -1 && content.length() - subContent.length() == endIndex) {
			return insert(index, content.substring(0, endIndex));
		}

		if (subContent.length() == startIndex) {
			return insert(index + startIndex, content.substring(startIndex));
		}

		if (startIndex != -1 && endIndex != -1 && content.length() - subContent.length() >= endIndex - startIndex) {
			return insert(index + startIndex,
					content.substring(startIndex, startIndex + content.length() - subContent.length()));
		}

		return null;
	}

	private static int getStartIndex(String subContent, String content) {
		int startIndex = -1;
		for (int i = 0; i < subContent.length(); i++) {
			if (content.charAt(i) == subContent.charAt(i)) {
				startIndex = i + 1;
			} else {
				break;
			}
		}
		return startIndex;
	}

	private static int getEndIndex(String subContent, String content) {
		int endIndex = -1;
		for (int i = content.length() - 1, j = subContent.length() - 1; i >= 0 && j >= 0; i--) {
			if (content.charAt(i) == subContent.charAt(j--)) {
				endIndex = i;
			} else {
				break;
			}
		}
		return endIndex;
	}
}
