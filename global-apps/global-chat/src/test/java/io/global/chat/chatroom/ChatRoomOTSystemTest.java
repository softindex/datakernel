package io.global.chat.chatroom;

import io.datakernel.ot.OTState;
import io.datakernel.ot.OTSystem;
import io.datakernel.ot.TransformResult;
import io.datakernel.ot.exceptions.OTTransformException;
import io.global.chat.chatroom.operation.CallOperation;
import io.global.chat.chatroom.operation.ChatRoomOperation;
import io.global.chat.chatroom.operation.DropCallOperation;
import io.global.chat.chatroom.operation.HandleCallOperation;
import io.global.common.KeyPair;
import io.global.common.PubKey;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.datakernel.util.CollectionUtils.map;
import static io.global.chat.chatroom.operation.CallOperation.call;
import static io.global.chat.chatroom.operation.DropCallOperation.dropCall;
import static io.global.chat.chatroom.operation.HandleCallOperation.accept;
import static io.global.chat.chatroom.operation.HandleCallOperation.reject;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.Assert.assertEquals;

public final class ChatRoomOTSystemTest {
	private static final OTSystem<ChatRoomOperation> SYSTEM = ChatRoomOTSystem.createOTSystem();
	private static final PubKey PUB_KEY_1 = KeyPair.generate().getPubKey();
	private static final PubKey PUB_KEY_2 = KeyPair.generate().getPubKey();
	private static final PubKey PUB_KEY_3 = KeyPair.generate().getPubKey();

	@Test
	public void transform2Calls() throws OTTransformException {
		CallOperation call1 = call(new CallInfo(PUB_KEY_1, 100));
		CallOperation call2 = call(new CallInfo(PUB_KEY_2, 200));

		ChatRoomOTState expectedState = new ChatRoomOTState();

		// The more recent call should win
		expectedState.apply(call2);
		testTransform(emptyList(), expectedState, call1, call2);
	}

	@Test
	public void transform2Calls1Inverted() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);

		CallOperation newCall = call(initialCallInfo, new CallInfo(PUB_KEY_1, 200));
		CallOperation invert = initialCall.invert();

		ChatRoomOTState expectedState = new ChatRoomOTState();

		// The new call should win
		expectedState.apply(initialCall);
		expectedState.apply(newCall);
		testTransform(singletonList(initialCall), expectedState, newCall, invert);
	}

	@Test
	public void transformCallAndDropCall() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);

		CallInfo newCallInfo = new CallInfo(PUB_KEY_2, 200);
		CallOperation newCall = call(initialCallInfo, newCallInfo);

		// new call should win always
		// no one accepted
		DropCallOperation dropCall = dropCall(initialCallInfo, emptyMap(), 250);
		ChatRoomOTState expectedState = new ChatRoomOTState();
		expectedState.apply(initialCall);
		expectedState.apply(newCall);
		testTransform(singletonList(initialCall), expectedState, newCall, dropCall);

		// someone accepted call
		dropCall = dropCall(initialCallInfo, map(PUB_KEY_1, TRUE), 250);
		expectedState.init();
		expectedState.apply(initialCall);
		HandleCallOperation acceptCall = accept(PUB_KEY_1);
		expectedState.apply(acceptCall);
		expectedState.apply(newCall);
		testTransform(asList(initialCall, acceptCall), expectedState, newCall, dropCall);
	}

	@Test
	public void transformInvertedCallAndDropCall() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);

		CallOperation invertedCall = initialCall.invert();
		DropCallOperation dropCall = dropCall(initialCallInfo, emptyMap(), 250);


		// drop should win
		ChatRoomOTState expectedState = new ChatRoomOTState();
		expectedState.apply(initialCall);
		expectedState.apply(dropCall);
		testTransform(singletonList(initialCall), expectedState, invertedCall, dropCall);
	}

	@Test
	public void transformCallAndInvertedDropCall() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);
		DropCallOperation initialDrop = dropCall(initialCallInfo, emptyMap(), 250);

		CallOperation newCall = call(new CallInfo(PUB_KEY_2, 300));
		DropCallOperation invertedDrop = initialDrop.invert();

		// new call should win
		ChatRoomOTState expectedState = new ChatRoomOTState();
		expectedState.apply(initialCall);
		expectedState.apply(initialDrop);
		expectedState.apply(newCall);
		testTransform(asList(initialCall, initialDrop), expectedState, invertedDrop, newCall);
	}

	@Test
	public void transformCallAndHandleCall() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);

		CallInfo newCallInfo = new CallInfo(PUB_KEY_2, 200);
		CallOperation newCall = call(initialCallInfo, newCallInfo);
		HandleCallOperation acceptCall = accept(PUB_KEY_1);

		// both should apply
		ChatRoomOTState expectedState = new ChatRoomOTState();
		expectedState.apply(initialCall);
		expectedState.apply(newCall);
		expectedState.apply(acceptCall);
		testTransform(singletonList(initialCall), expectedState, newCall, acceptCall);
	}

	@Test
	public void transformCallAndInvertedHandleCall() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);
		HandleCallOperation initialAccept = accept(PUB_KEY_1);

		CallOperation newCall = call(initialCallInfo, new CallInfo(PUB_KEY_2, 200));
		HandleCallOperation invertedAcceptCall = initialAccept.invert();

		// both should apply
		ChatRoomOTState expectedState = new ChatRoomOTState();
		expectedState.apply(initialCall);
		expectedState.apply(newCall);
		testTransform(asList(initialCall, initialAccept), expectedState, newCall, invertedAcceptCall);
	}

	@Test
	public void transformInvertedCallAndHandleCall() throws OTTransformException {
		// prev call is null
		CallOperation initialCall = call(new CallInfo(PUB_KEY_2, 200));

		CallOperation invertedCall = initialCall.invert();
		HandleCallOperation acceptCall = accept(PUB_KEY_1);

		// handle does not apply
		ChatRoomOTState expectedState = new ChatRoomOTState();
		expectedState.apply(initialCall);
		expectedState.apply(invertedCall);
		testTransform(singletonList(initialCall), expectedState, invertedCall, acceptCall);

		// prev call is not null
		CallInfo firstCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation firstCall = call(firstCallInfo);
		CallOperation secondCall = call(firstCallInfo, new CallInfo(PUB_KEY_2, 200));

		invertedCall = secondCall.invert();

		// handle does apply
		expectedState.init();
		expectedState.apply(firstCall);
		expectedState.apply(acceptCall);
		testTransform(asList(firstCall, secondCall), expectedState, invertedCall, acceptCall);
	}

	@Test
	public void transform2Drops() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);

		DropCallOperation dropCall1 = dropCall(initialCallInfo, emptyMap(), 200);
		DropCallOperation dropCall2 = dropCall(initialCallInfo, emptyMap(), 300);

		ChatRoomOTState expectedState = new ChatRoomOTState();

		// The earlier drop should win
		expectedState.apply(initialCall);
		expectedState.apply(dropCall1);
		testTransform(singletonList(initialCall), expectedState, dropCall1, dropCall2);
	}

	@Test
	public void transformDropAndHandle() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);

		DropCallOperation dropCall = dropCall(initialCallInfo, emptyMap(), 200);
		HandleCallOperation acceptCall = accept(PUB_KEY_1);

		ChatRoomOTState expectedState = new ChatRoomOTState();

		// Drop should win
		expectedState.apply(initialCall);
		expectedState.apply(dropCall);
		testTransform(singletonList(initialCall), expectedState, dropCall, acceptCall);
	}

	@Test
	public void transformDropAndInvertedHandle() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);
		HandleCallOperation accept1 = accept(PUB_KEY_1);
		HandleCallOperation accept2 = accept(PUB_KEY_2);

		Map<PubKey, Boolean> handled = map(PUB_KEY_1, TRUE, PUB_KEY_2, TRUE);
		DropCallOperation dropCall = dropCall(initialCallInfo, handled, 200);
		HandleCallOperation invertedAcceptCall = accept2.invert();

		ChatRoomOTState expectedState = new ChatRoomOTState();

		// Drop should win
		expectedState.apply(initialCall);
		expectedState.apply(accept1);
		expectedState.apply(accept2);
		expectedState.apply(dropCall);
		testTransform(asList(initialCall, accept1, accept2), expectedState, dropCall, invertedAcceptCall);
	}

	@Test
	public void transform2Handles() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);

		HandleCallOperation accept1 = accept(PUB_KEY_1);
		HandleCallOperation accept2 = accept(PUB_KEY_2);

		ChatRoomOTState expectedState = new ChatRoomOTState();

		// Both should apply
		expectedState.apply(initialCall);
		expectedState.apply(accept1);
		expectedState.apply(accept2);
		testTransform(singletonList(initialCall), expectedState, accept1, accept2);
	}

	@Test
	public void transformAcceptRejectSameUser() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);

		HandleCallOperation accept = accept(PUB_KEY_1);
		HandleCallOperation reject = reject(PUB_KEY_1);

		ChatRoomOTState expectedState = new ChatRoomOTState();

		// Accept wins
		expectedState.apply(initialCall);
		expectedState.apply(accept);
		testTransform(singletonList(initialCall), expectedState, accept, reject);
	}

	@Test
	public void transform2Handles1Inverted() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);
		HandleCallOperation initialAccept = accept(PUB_KEY_1);

		HandleCallOperation accept = accept(PUB_KEY_2);
		HandleCallOperation invertedInitialAccept = initialAccept.invert();

		ChatRoomOTState expectedState = new ChatRoomOTState();

		// Not inverted accept should be applied, inverted should be reverted
		expectedState.apply(initialCall);
		expectedState.apply(accept);
		testTransform(asList(initialCall, initialAccept), expectedState, accept, invertedInitialAccept);
	}

	@Test
	public void transform2HandlesBothInverted() throws OTTransformException {
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);
		CallOperation initialCall = call(initialCallInfo);
		HandleCallOperation initialAccept1 = accept(PUB_KEY_1);
		HandleCallOperation initialAccept2 = accept(PUB_KEY_2);

		HandleCallOperation invertedAccept1 = initialAccept1.invert();
		HandleCallOperation invertedAccept2 = initialAccept2.invert();

		ChatRoomOTState expectedState = new ChatRoomOTState();

		// Both handles should be reverted
		expectedState.apply(initialCall);
		testTransform(asList(initialCall, initialAccept1, initialAccept2), expectedState, invertedAccept1, invertedAccept2);
	}

	@Test
	public void squashDropAndHandle() {
		ChatRoomOTState stateNotSquashed = new ChatRoomOTState();
		ChatRoomOTState stateSquashed = new ChatRoomOTState();
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);

		CallOperation initialCall = call(initialCallInfo);
		HandleCallOperation initialAccept = accept(PUB_KEY_1);
		HandleCallOperation accept = accept(PUB_KEY_2);
		Map<PubKey, Boolean> handled = map(PUB_KEY_1, TRUE, PUB_KEY_2, TRUE);
		DropCallOperation drop = dropCall(initialCallInfo, handled, 200);

		List<ChatRoomOperation> initialOps = asList(initialCall, initialAccept, accept, drop);
		doApply(stateNotSquashed, stateSquashed, initialOps);

		DropCallOperation dropInvert = drop.invert();
		HandleCallOperation reject = reject(PUB_KEY_3);

		// accept not inverted
		stateNotSquashed.apply(dropInvert);
		stateNotSquashed.apply(reject);

		List<ChatRoomOperation> squash1 = SYSTEM.squash(asList(dropInvert, reject));
		assertEquals(1, squash1.size());
		squash1.forEach(stateSquashed::apply);

		assertEquals(stateNotSquashed, stateSquashed);

		stateNotSquashed.init();
		stateSquashed.init();

		doApply(stateNotSquashed, stateSquashed, initialOps);
		// accept inverted
		HandleCallOperation handleInvert = accept.invert();
		stateNotSquashed.apply(dropInvert);
		stateNotSquashed.apply(handleInvert);

		List<ChatRoomOperation> squash2 = SYSTEM.squash(asList(dropInvert, handleInvert));
		assertEquals(1, squash2.size());
		squash2.forEach(stateSquashed::apply);

		assertEquals(stateNotSquashed, stateSquashed);
	}

	@Test
	public void squashHandleAndDrop() {
		ChatRoomOTState stateNotSquashed = new ChatRoomOTState();
		ChatRoomOTState stateSquashed = new ChatRoomOTState();
		CallInfo initialCallInfo = new CallInfo(PUB_KEY_1, 100);

		CallOperation initialCall = call(initialCallInfo);
		HandleCallOperation initialAccept = accept(PUB_KEY_1);
		HandleCallOperation accept = accept(PUB_KEY_2);

		List<ChatRoomOperation> initialOps = asList(initialCall, initialAccept, accept);
		doApply(stateNotSquashed, stateSquashed, initialOps);

		HandleCallOperation reject = reject(PUB_KEY_3);
		Map<PubKey, Boolean> handled = map(PUB_KEY_1, TRUE, PUB_KEY_2, TRUE, PUB_KEY_3, FALSE);
		DropCallOperation drop1 = dropCall(initialCallInfo, handled, 200);

		// accept not inverted
		stateNotSquashed.apply(reject);
		stateNotSquashed.apply(drop1);

		List<ChatRoomOperation> squash1 = SYSTEM.squash(asList(reject, drop1));
		assertEquals(1, squash1.size());
		squash1.forEach(stateSquashed::apply);

		assertEquals(stateNotSquashed, stateSquashed);

		stateNotSquashed.init();
		stateSquashed.init();

		DropCallOperation drop2 = dropCall(initialCallInfo, map(PUB_KEY_1, TRUE), 200);
		doApply(stateNotSquashed, stateSquashed, initialOps);
		// accept inverted
		HandleCallOperation acceptInvert = accept.invert();
		stateNotSquashed.apply(acceptInvert);
		stateNotSquashed.apply(drop2);

		List<ChatRoomOperation> squash2 = SYSTEM.squash(asList(acceptInvert, drop2));
		assertEquals(1, squash2.size());
		squash2.forEach(stateSquashed::apply);

		assertEquals(stateNotSquashed, stateSquashed);
	}

	private static void doApply(OTState<ChatRoomOperation> state1, OTState<ChatRoomOperation> state2, List<ChatRoomOperation> ops) {
		ops.forEach(state1::apply);
		ops.forEach(state2::apply);
	}

	private static void testTransform(List<ChatRoomOperation> initialOps, ChatRoomOTState expectedState, ChatRoomOperation op1, ChatRoomOperation op2) throws OTTransformException {
		doTestTransform(initialOps, expectedState, op1, op2);
		doTestTransform(initialOps, expectedState, op2, op1);
	}

	private static void doTestTransform(List<ChatRoomOperation> initialOps, ChatRoomOTState expectedState, ChatRoomOperation leftOp, ChatRoomOperation rightOp) throws OTTransformException {
		ChatRoomOTState leftState = new ChatRoomOTState();
		ChatRoomOTState rightState = new ChatRoomOTState();
		initialOps.forEach(leftState::apply);
		initialOps.forEach(rightState::apply);
		leftState.apply(leftOp);
		rightState.apply(rightOp);
		TransformResult<ChatRoomOperation> transform = SYSTEM.transform(leftOp, rightOp);
		transform.left.forEach(leftState::apply);
		transform.right.forEach(rightState::apply);
		assertEquals(leftState, rightState);
		assertEquals(leftState, expectedState);
	}
}
