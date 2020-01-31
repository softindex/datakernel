package io.global.chat.chatroom;

import io.datakernel.ot.OTSystem;
import io.datakernel.ot.OTSystemImpl;
import io.datakernel.ot.OTSystemImpl.SquashFunction;
import io.datakernel.ot.OTSystemImpl.TransformFunction;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;
import io.global.chat.chatroom.operation.*;
import io.global.common.PubKey;
import io.global.ot.map.MapOperation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static io.datakernel.common.collection.CollectionUtils.first;
import static io.global.chat.Utils.HANDLE_CALL_SUBSYSTEM;
import static io.global.chat.chatroom.operation.DropCallOperation.dropCall;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public final class ChatRoomOTSystem {
	private ChatRoomOTSystem() {
		throw new AssertionError();
	}

	public static OTSystem<ChatRoomOperation> createOTSystem() {

		return OTSystemImpl.<ChatRoomOperation>create()
				.withEmptyPredicate(MessageOperation.class, MessageOperation::isEmpty)
				.withEmptyPredicate(CallOperation.class, CallOperation::isEmpty)
				.withEmptyPredicate(DropCallOperation.class, DropCallOperation::isEmpty)
				.withEmptyPredicate(HandleCallOperation.class, HandleCallOperation::isEmpty)

				.withInvertFunction(MessageOperation.class, op -> singletonList(op.invert()))
				.withInvertFunction(CallOperation.class, op -> singletonList(op.invert()))
				.withInvertFunction(DropCallOperation.class, op -> singletonList(op.invert()))
				.withInvertFunction(HandleCallOperation.class, op -> singletonList(op.invert()))

				.withTransformFunction(MessageOperation.class, MessageOperation.class, swap())
				.withTransformFunction(MessageOperation.class, CallOperation.class, swap())
				.withTransformFunction(MessageOperation.class, DropCallOperation.class, swap())
				.withTransformFunction(MessageOperation.class, HandleCallOperation.class, swap())

				.withTransformFunction(CallOperation.class, CallOperation.class, (left, right) -> {
					if (!Objects.equals(left.getPrev(), right.getPrev())) {
						throw new OTTransformException("Previous values should be equal");
					}

					CallInfo leftNextValue = left.getNext();
					CallInfo rightNextValue = right.getNext();

					if (Objects.equals(leftNextValue, rightNextValue)) {
						return TransformResult.empty();
					}

					if (leftNextValue == null) {
						return TransformResult.left(asList(left.invert(), right));
					}
					if (rightNextValue == null) {
						return TransformResult.right(asList(right.invert(), left));
					}
					if (leftNextValue.getCallStarted() >= rightNextValue.getCallStarted()) {
						return TransformResult.right(asList(right.invert(), left));
					} else {
						return TransformResult.left(asList(left.invert(), right));
					}
				})
				.withTransformFunction(CallOperation.class, DropCallOperation.class, (left, right) -> {
					if (left.getNext() != null) {
						return TransformResult.right(asList(right.invert(), left));
					}
					if (!right.isInvert()) {
						return TransformResult.left(asList(left.invert(), right));
					}
					throw new OTTransformException();
				})
				.withTransformFunction(CallOperation.class, HandleCallOperation.class, (left, right) -> {
					if (left.getNext() == null) {
						return TransformResult.right(asList(right.invert(), left));
					}
					return TransformResult.of(right, left);
				})

				.withTransformFunction(DropCallOperation.class, DropCallOperation.class, (left, right) -> {
					if (left.equals(right)) {
						return TransformResult.empty();
					}
					if (!left.isInvert() && !right.isInvert()) {
						if (left.getDropTimestamp() <= right.getDropTimestamp()) {
							return TransformResult.right(asList(right.invert(), left));
						} else {
							return TransformResult.left(asList(left.invert(), right));
						}
					}
					throw new OTTransformException();
				})
				.withTransformFunction(DropCallOperation.class, HandleCallOperation.class, (left, right) -> {
					if (!left.isInvert()) {
						CallInfo callInfo = left.getCallInfo();
						Map<PubKey, Boolean> previouslyHandled = new HashMap<>(left.getPreviouslyHandled());

						right.getMapOperation().getOperations()
								.forEach((user, setValue) -> {
									Boolean next = setValue.getNext();
									if (next == null) {
										previouslyHandled.remove(user);
									} else {
										previouslyHandled.put(user, next);
									}
								});
						long dropTimestamp = left.getDropTimestamp();
						return TransformResult.right(dropCall(callInfo, previouslyHandled, dropTimestamp));
					}
					throw new OTTransformException();
				})

				.withTransformFunction(HandleCallOperation.class, HandleCallOperation.class, (left, right) -> {
					TransformResult<MapOperation<PubKey, Boolean>> transform = HANDLE_CALL_SUBSYSTEM
							.transform(left.getMapOperation(), right.getMapOperation());
					return TransformResult.of(
							collect(transform.left, HandleCallOperation::new),
							collect(transform.right, HandleCallOperation::new)
					);
				})

				.withSquashFunction(MessageOperation.class, MessageOperation.class, doSquash((first, second) -> {
					if (first.isInversionFor(second)) {
						return MessageOperation.EMPTY;
					}
					return null;
				}))
				.withSquashFunction(CallOperation.class, CallOperation.class, doSquash((first, second) -> {
					if (first.getPrev() != null && first.getNext() != null && second.getPrev() != null && second.getNext() != null) {
						return new CallOperation(first.getPrev(), second.getNext());
					}
					return null;
				}))
				.withSquashFunction(DropCallOperation.class, DropCallOperation.class, doSquash((first, second) -> {
					if (first.isInversionFor(second)) {
						return DropCallOperation.EMPTY;
					}
					return null;
				}))
				.withSquashFunction(HandleCallOperation.class, HandleCallOperation.class, (first, second) -> {
					List<MapOperation<PubKey, Boolean>> toBeSquashed = asList(first.getMapOperation(), second.getMapOperation());
					List<MapOperation<PubKey, Boolean>> squashed = HANDLE_CALL_SUBSYSTEM.squash(toBeSquashed);
					return new HandleCallOperation(first(squashed));
				})
				.withSquashFunction(DropCallOperation.class, HandleCallOperation.class, doSquash((first, second) -> {
					if (first.isInvert()) {
						Map<PubKey, Boolean> previouslyHandled = new HashMap<>(first.getPreviouslyHandled());

						second.getMapOperation().getOperations()
								.forEach((user, setValue) -> {
									Boolean next = setValue.getNext();
									if (next == null) {
										previouslyHandled.remove(user);
									} else {
										previouslyHandled.put(user, next);
									}
								});
						return new DropCallOperation(first.getCallInfo(), previouslyHandled, first.getDropTimestamp(), true);
					}
					return null;
				}))
				.withSquashFunction(HandleCallOperation.class, DropCallOperation.class, doSquash((first, second) -> {
					if (!second.isInvert()) {
						Map<PubKey, Boolean> previouslyHandled = new HashMap<>(second.getPreviouslyHandled());

						first.getMapOperation().getOperations()
								.forEach((user, setValue) -> {
									if (setValue.getNext() == null) {
										previouslyHandled.put(user, setValue.getPrev());
									} else {
										previouslyHandled.remove(user);
									}
								});
						return new DropCallOperation(second.getCallInfo(), previouslyHandled, second.getDropTimestamp(), false);
					}
					return null;
				}));
	}

	private static <L extends ChatRoomOperation, R extends ChatRoomOperation> TransformFunction<ChatRoomOperation, L, R> swap() {
		return (left, right) -> {
			if (left.equals(right)) {
				return TransformResult.empty();
			}
			return TransformResult.of(right, left);
		};
	}

	private static <O1 extends ChatRoomOperation, O2 extends ChatRoomOperation> SquashFunction<ChatRoomOperation, O1, O2> doSquash(SquashFunction<ChatRoomOperation, O1, O2> squashFn) {
		return (first, second) -> {
			if (first.isEmpty()) return second;
			if (second.isEmpty()) return first;
			return squashFn.trySquash(first, second);
		};
	}

	private static <O, S> List<O> collect(List<S> ops, Function<S, O> constructor) {
		return ops.stream().map(constructor).collect(toList());
	}
}
