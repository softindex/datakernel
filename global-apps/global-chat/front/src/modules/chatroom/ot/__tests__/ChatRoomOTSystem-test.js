import chatRoomOTSystem from '../ChatRoomOTSystem';
import CallOperation from '../CallOperation';
import DropCallOperation from '../DropCallOperation';
import HandleCallOperation from '../HandleCallOperation';
import initOTState from '../initOTState';
import * as types from '../../MESSAGE_TYPES';

const PUB_KEY_1 = 'pubkey1';
const PUB_KEY_2 = 'pubkey2';
const PUB_KEY_3 = 'pubkey3';
const initialCallInfo = {
  pubKey: PUB_KEY_1,
  peerId: 1,
  timestamp: 100
};

function initState({messages, call} = {}) {
  return {
    ...initOTState(),
    ...(messages ? {messages} : {}),
    ...(call ? {
        call: {
          ...initOTState().call,
          ...call
        }
      } : {}
    )
  };
}

function testTransform(initialOps, expectedState, operationLeft, operationRight) {
  doTestTransform(initialOps, expectedState, operationLeft, operationRight);
  doTestTransform(initialOps, expectedState, operationRight, operationLeft);
}

function doTestTransform(initialOps = [], expectedState, operationLeft, operationRight) {
  const leftState = initState();
  const rightState = initState();

  for (const operation of initialOps) {
    operation.apply(leftState);
    operation.apply(rightState);
  }

  operationLeft.apply(leftState);
  operationRight.apply(rightState);

  const transform = chatRoomOTSystem.transform([operationLeft], [operationRight]);

  for (const operation of transform.leftOps) {
    operation.apply(leftState)
  }

  for (const operation of transform.rightOps) {
    operation.apply(rightState);
  }

  expect(leftState).toEqual(rightState);
  expect(leftState).toEqual(expectedState);
}

function doApply(state1, state2, ops) {
  for (const operation of ops) {
    operation.apply(state1);
    operation.apply(state2);
  }
}

describe('ChatRoomOTSystem', () => {
  describe('CallOperation transform', () => {
    it('Should transform two calls and the more recent call should win', () => {
      const newCallInfo = {
        pubKey: PUB_KEY_2,
        peerId: 2,
        timestamp: 200
      };
      const call1 = CallOperation.create(initialCallInfo);
      const call2 = CallOperation.create(newCallInfo);

      testTransform(
        [],
        initState({
          call: {
            callerInfo: {
              publicKey: newCallInfo.pubKey,
              peerId: newCallInfo.peerId
            },
            timestamp: newCallInfo.timestamp
          },
          messages: new Set([JSON.stringify({
            timestamp: newCallInfo.timestamp,
            authorPublicKey: newCallInfo.pubKey,
            authorPeerId: newCallInfo.peerId,
            type: types.MESSAGE_CALL
          })])
        }),
        call1,
        call2
      );
    });

    it('Should transform two call with one inverted and the new call should win', () => {
      const newCallInfo = {
        pubKey: PUB_KEY_2,
        peerId: 2,
        timestamp: 200
      };
      const initialCall = CallOperation.create(initialCallInfo);
      const newCall = new CallOperation(initialCallInfo, newCallInfo);
      const invertedCall = initialCall.invert();

      testTransform(
        [initialCall],
        initState({
          call: {
            callerInfo: {
              publicKey: newCallInfo.pubKey,
              peerId: newCallInfo.peerId
            },
            timestamp: newCallInfo.timestamp
          },
          messages: new Set([JSON.stringify({
            timestamp: initialCallInfo.timestamp,
            authorPublicKey: initialCallInfo.pubKey,
            authorPeerId: initialCallInfo.peerId,
            type: types.MESSAGE_CALL
          })])
        }),
        newCall,
        invertedCall
      );
    });
  });

  describe('CallOperation and DropCallOperation transform', () => {
    it ('Should transform calls and the new call should win', () => {
      const newCallInfo = {
        pubKey: PUB_KEY_2,
        peerId: 2,
        timestamp: 200
      };
      const initialCall = CallOperation.create(initialCallInfo);
      const newCall = new CallOperation(initialCallInfo, newCallInfo);
      const dropCall = DropCallOperation.create(
        initialCallInfo.pubKey,
        initialCallInfo.peerId,
        initialCallInfo.timestamp,
        new Map(),
        250
      );

      testTransform(
        [initialCall],
        initState({
          call: {
            callerInfo: {
              publicKey: newCallInfo.pubKey,
              peerId: newCallInfo.peerId
            },
            timestamp: newCallInfo.timestamp
          },
          messages: new Set([JSON.stringify({
            timestamp: initialCallInfo.timestamp,
            authorPublicKey: initialCallInfo.pubKey,
            authorPeerId: initialCallInfo.peerId,
            type: types.MESSAGE_CALL
          })])
        }),
        newCall,
        dropCall
      );
    });

    it('Someone accepted call, the new call should win', () => {
      const newCallInfo = {
        pubKey: PUB_KEY_2,
        peerId: 2,
        timestamp: 200
      };
      const initialCall = CallOperation.create(initialCallInfo);
      const newCall = new CallOperation(initialCallInfo, newCallInfo);
      const dropCall = DropCallOperation.create(
        initialCallInfo.pubKey,
        initialCallInfo.peerId,
        initialCallInfo.timestamp,
        new Map([[PUB_KEY_1, true]]),
        250
      );
      const acceptCall = HandleCallOperation.accept(PUB_KEY_1, null);

      testTransform(
        [initialCall, acceptCall],
        {
          call: {
            callerInfo: {
              publicKey: newCallInfo.pubKey,
              peerId: newCallInfo.peerId
            },
            timestamp: newCallInfo.timestamp,
            handled: new Map([[PUB_KEY_1, true]])
          },
          messages: new Set([JSON.stringify({
            timestamp: initialCallInfo.timestamp,
            authorPublicKey: initialCallInfo.pubKey,
            authorPeerId: initialCallInfo.peerId,
            type: types.MESSAGE_CALL
          })])
        },
        newCall,
        dropCall);
    });

    it('Should transform inverted call and drop call, the drop call should win', () => {
      const initialCall = CallOperation.create(initialCallInfo);
      const invertedCall = initialCall.invert();
      const dropCall = DropCallOperation.create(
        initialCallInfo.pubKey,
        initialCallInfo.peerId,
        initialCallInfo.timestamp,
        new Map(),
        250
      );

      testTransform(
        [initialCall],
        initState({
          messages: new Set([
            JSON.stringify({
              timestamp: initialCallInfo.timestamp,
              authorPublicKey: initialCallInfo.pubKey,
              authorPeerId: initialCallInfo.peerId,
              type: types.MESSAGE_CALL
            }),
            JSON.stringify({
              timestamp: 250,
              authorPublicKey: initialCallInfo.pubKey,
              type: types.MESSAGE_DROP
            })
          ])
        }),
        invertedCall,
        dropCall
      );
    });

    it('Should transform call and inverted drop call, the new call should win', () => {
      const newCallInfo = {
        pubKey: PUB_KEY_2,
        peerId: 2,
        timestamp: 300
      };
      const initialCall = CallOperation.create(initialCallInfo);
      const initialDropCall = DropCallOperation.create(
        initialCallInfo.pubKey,
        initialCallInfo.peerId,
        initialCallInfo.timestamp,
        new Map(),
        250
      );
      const newCall = CallOperation.create(newCallInfo);
      const invertedDropCall = initialDropCall.invert();

      testTransform(
        [initialCall, initialDropCall],
        initState({
          call: {
            callerInfo: {
              publicKey: newCallInfo.pubKey,
              peerId: newCallInfo.peerId
            },
            timestamp: newCallInfo.timestamp
          },
          messages: new Set([
            JSON.stringify({
              timestamp: initialCallInfo.timestamp,
              authorPublicKey: initialCallInfo.pubKey,
              authorPeerId: initialCallInfo.peerId,
              type: types.MESSAGE_CALL
            }),
            JSON.stringify({
              timestamp: 250,
              authorPublicKey: initialCallInfo.pubKey,
              type: types.MESSAGE_DROP
            }),
            JSON.stringify({
              timestamp: newCallInfo.timestamp,
              authorPublicKey: newCallInfo.pubKey,
              authorPeerId: newCallInfo.peerId,
              type: types.MESSAGE_CALL
            })
          ])
        }),
        invertedDropCall,
        newCall
      );
    });
  });

  describe('CallOperation and HandleCallOperation transform', () => {
    it('Should transform calls, both should apply', () => {
      const newCallInfo = {
        pubKey: PUB_KEY_2,
        peerId: 2,
        timestamp: 200
      };
      const initialCall = CallOperation.create(initialCallInfo);
      const newCall = new CallOperation(initialCallInfo, newCallInfo);
      const acceptCall = HandleCallOperation.accept(PUB_KEY_1, null);

      testTransform(
        [initialCall],
        initState({
          call: {
            callerInfo: {
              publicKey: newCallInfo.pubKey,
              peerId: newCallInfo.peerId
            },
            timestamp: newCallInfo.timestamp,
            handled: new Map([[PUB_KEY_1, true]])
          },
          messages: new Set([JSON.stringify({
            timestamp: initialCallInfo.timestamp,
            authorPublicKey: initialCallInfo.pubKey,
            authorPeerId: initialCallInfo.peerId,
            type: types.MESSAGE_CALL
          })])
        }),
        newCall,
        acceptCall
      );
    });

    it('Should transform call and inverted handle call, both should apply', () => {
      const newCallInfo = {
        pubKey: PUB_KEY_2,
        peerId: 2,
        timestamp: 200
      };
      const initialCall = CallOperation.create(initialCallInfo);
      const newCall = new CallOperation(initialCallInfo, newCallInfo);
      const initialAcceptCall = HandleCallOperation.accept(PUB_KEY_1, null);
      const invertedAcceptCall = initialAcceptCall.invert();

      testTransform(
        [initialCall, initialAcceptCall],
        initState({
          call: {
            callerInfo: {
              publicKey: newCallInfo.pubKey,
              peerId: newCallInfo.peerId
            },
            timestamp: newCallInfo.timestamp
          },
          messages: new Set([JSON.stringify({
            timestamp: initialCallInfo.timestamp,
            authorPublicKey: initialCallInfo.pubKey,
            authorPeerId: initialCallInfo.peerId,
            type: types.MESSAGE_CALL
          })])
        }),
        newCall,
        invertedAcceptCall
      );
    });

    it('Should transform inverted call and handle call, handle call does not apply', () => {
      const initialCall = CallOperation.create(initialCallInfo);
      const invertedCall = initialCall.invert();
      const acceptCall = HandleCallOperation.accept(PUB_KEY_1, null);

      testTransform([initialCall], initState(), invertedCall, acceptCall);
    });

    it('Should transform inverted call and handle call, handle call does apply', () => {
      const firstCallInfo = {...initialCallInfo};
      const secondCallInfo = {
        pubKey: PUB_KEY_2,
        peerId: 2,
        timestamp: 200
      };
      const firstCall = CallOperation.create(firstCallInfo);
      const secondCall = new CallOperation(firstCallInfo, secondCallInfo);
      const invertedCall = secondCall.invert();
      const acceptCall = HandleCallOperation.accept(PUB_KEY_1, null);

      testTransform(
        [firstCall, secondCall],
        initState({
          call: {
            callerInfo: {
              publicKey: firstCallInfo.pubKey,
              peerId: firstCallInfo.peerId
            },
            timestamp: firstCallInfo.timestamp,
            handled: new Map([[PUB_KEY_1, true]])
          },
          messages: new Set([JSON.stringify({
            timestamp: firstCallInfo.timestamp,
            authorPublicKey: firstCallInfo.pubKey,
            authorPeerId: firstCallInfo.peerId,
            type: types.MESSAGE_CALL
          })])
        }),
        invertedCall,
        acceptCall
      );
    });
  });

  describe('DropCallOperation transform', () => {
    it('Should transform two drop calls, the earlier drop should win', () => {
      const initialCall = CallOperation.create(initialCallInfo);
      const dropCall1 = DropCallOperation.create(
        initialCallInfo.pubKey,
        initialCallInfo.peerId,
        initialCallInfo.timestamp,
        new Map(),
        200
      );
      const dropCall2 = DropCallOperation.create(
        initialCallInfo.pubKey,
        initialCallInfo.peerId,
        initialCallInfo.timestamp,
        new Map(),
        300
      );

      testTransform(
        [initialCall],
        initState({
          messages: new Set([
            JSON.stringify({
              timestamp: initialCallInfo.timestamp,
              authorPublicKey: initialCallInfo.pubKey,
              authorPeerId: initialCallInfo.peerId,
              type: types.MESSAGE_CALL
            }),
            JSON.stringify({
              timestamp: 200,
              authorPublicKey: initialCallInfo.pubKey,
              type: types.MESSAGE_DROP
            })
          ])
        }),
        dropCall1,
        dropCall2
      );
    });
  });

  describe('DropCallOperation and HandleCallOperation transform', () => {
    it('Should transform calls, drop should win', () => {
      const initialCall = CallOperation.create(initialCallInfo);
      const dropCall = DropCallOperation.create(
        initialCallInfo.pubKey,
        initialCallInfo.peerId,
        initialCallInfo.timestamp,
        new Map(),
        200
      );
      const acceptCall = HandleCallOperation.accept(PUB_KEY_1, null);

      testTransform(
        [initialCall],
        initState({
          messages: new Set([
            JSON.stringify({
              timestamp: initialCallInfo.timestamp,
              authorPublicKey: initialCallInfo.pubKey,
              authorPeerId: initialCallInfo.peerId,
              type: types.MESSAGE_CALL
            }),
            JSON.stringify({
              timestamp: 200,
              authorPublicKey: initialCallInfo.pubKey,
              type: types.MESSAGE_DROP
            })
          ])
        }),
        dropCall,
        acceptCall
      );
    });

    it('Should transform drop call and inverted handle call, drop should win', () => {
      const initialCall = CallOperation.create(initialCallInfo);
      const acceptCall1 = HandleCallOperation.accept(PUB_KEY_1, null);
      const acceptCall2 = HandleCallOperation.accept(PUB_KEY_2, null);
      const handled = new Map([[PUB_KEY_1, true], [PUB_KEY_2, true]]);
      const dropCall = DropCallOperation.create(
        initialCallInfo.pubKey,
        initialCallInfo.peerId,
        initialCallInfo.timestamp,
        handled,
        200
      );
      const invertedAcceptCall = acceptCall2.invert();

      testTransform(
        [initialCall, acceptCall1, acceptCall2],
        initState({
          messages: new Set([
            JSON.stringify({
              timestamp: initialCallInfo.timestamp,
              authorPublicKey: initialCallInfo.pubKey,
              authorPeerId: initialCallInfo.peerId,
              type: types.MESSAGE_CALL
            }),
            JSON.stringify({
              timestamp: 200,
              authorPublicKey: initialCallInfo.pubKey,
              type: types.MESSAGE_DROP
            })
          ])
        }),
        dropCall,
        invertedAcceptCall
      );
    });
  });

  describe('HandleCallOperation transform', () => {
    it('Should transform two handle calls, both should apply', () => {
      const initialCall = CallOperation.create(initialCallInfo);
      const acceptCall1 = HandleCallOperation.accept(PUB_KEY_1, null);
      const acceptCall2 = HandleCallOperation.accept(PUB_KEY_2, null);

      testTransform(
        [initialCall],
        initState({
          call: {
            callerInfo: {
              publicKey: initialCallInfo.pubKey,
              peerId: initialCallInfo.peerId
            },
            timestamp: initialCallInfo.timestamp,
            handled: new Map([[PUB_KEY_1, true], [PUB_KEY_2, true]])
          },
          messages: new Set([JSON.stringify({
            timestamp: initialCallInfo.timestamp,
            authorPublicKey: initialCallInfo.pubKey,
            authorPeerId: initialCallInfo.peerId,
            type: types.MESSAGE_CALL
          })])
        }),
        acceptCall1,
        acceptCall2
      );
    });

    it('Should transform accept and reject handle calls, accept should win', () => {
      const initialCall = CallOperation.create(initialCallInfo);
      const acceptCall = HandleCallOperation.accept(PUB_KEY_1, null);
      const rejectCall = HandleCallOperation.reject(PUB_KEY_1, null);

      testTransform(
        [initialCall],
        initState({
          call: {
            callerInfo: {
              publicKey: initialCallInfo.pubKey,
              peerId: initialCallInfo.peerId
            },
            timestamp: initialCallInfo.timestamp,
            handled: new Map([[PUB_KEY_1, true]])
          },
          messages: new Set([JSON.stringify({
            timestamp: initialCallInfo.timestamp,
            authorPublicKey: initialCallInfo.pubKey,
            authorPeerId: initialCallInfo.peerId,
            type: types.MESSAGE_CALL
          })])
        }),
        acceptCall,
        rejectCall
      );
    });

    it('Should transform handle calls, one is inverted, not inverted should be applied', () => {
      const initialCall = CallOperation.create(initialCallInfo);
      const initialAcceptCall = HandleCallOperation.accept(PUB_KEY_1, null);
      const acceptCall = HandleCallOperation.accept(PUB_KEY_2, null);
      const invertedInitialAcceptCall = initialAcceptCall.invert();

      testTransform(
        [initialCall, initialAcceptCall],
        initState({
          call: {
            callerInfo: {
              publicKey: initialCallInfo.pubKey,
              peerId: initialCallInfo.peerId
            },
            timestamp: initialCallInfo.timestamp,
            handled: new Map([[PUB_KEY_2, true]])
          },
          messages: new Set([JSON.stringify({
            timestamp: initialCallInfo.timestamp,
            authorPublicKey: initialCallInfo.pubKey,
            authorPeerId: initialCallInfo.peerId,
            type: types.MESSAGE_CALL
          })])
        }),
        acceptCall,
        invertedInitialAcceptCall
      );
    });

    it('Should transform two handle calls, both are inverted and should be reverted', () => {
      const initialCall = CallOperation.create(initialCallInfo);
      const acceptCall1 = HandleCallOperation.accept(PUB_KEY_1, null);
      const acceptCall2 = HandleCallOperation.accept(PUB_KEY_2, null);
      const invertedAcceptCall1 = acceptCall1.invert();
      const invertedAcceptCall2 = acceptCall2.invert();

      testTransform(
        [initialCall, acceptCall1, acceptCall2],
        initState({
          call: {
            callerInfo: {
              publicKey: initialCallInfo.pubKey,
              peerId: initialCallInfo.peerId
            },
            timestamp: initialCallInfo.timestamp
          },
          messages: new Set([JSON.stringify({
            timestamp: initialCallInfo.timestamp,
            authorPublicKey: initialCallInfo.pubKey,
            authorPeerId: initialCallInfo.peerId,
            type: types.MESSAGE_CALL
          })])
        }),
        invertedAcceptCall1,
        invertedAcceptCall2
      );
    });
  });

  describe('DropCallOperation and HandleCallOperation squash', () => {
    const initialCall = CallOperation.create(initialCallInfo);
    const initialAcceptCall = HandleCallOperation.accept(PUB_KEY_1, null);
    const acceptCall = HandleCallOperation.accept(PUB_KEY_2, null);
    const handled = new Map([[PUB_KEY_1, true], [PUB_KEY_2, true]]);
    const dropCall = DropCallOperation.create(
      initialCallInfo.pubKey,
      initialCallInfo.peerId,
      initialCallInfo.timestamp,
      handled,
      200
    );

    it('Should squash calls, accept call should not be inverted', () => {
      const stateNotSquashed = initState();
      const stateSquashed = initState();

      doApply(stateNotSquashed, stateSquashed, [initialCall, initialAcceptCall, acceptCall, dropCall]);
      const invertedDropCall = dropCall.invert();
      const rejectCall = HandleCallOperation.reject(PUB_KEY_3, null);
      invertedDropCall.apply(stateNotSquashed);
      rejectCall.apply(stateNotSquashed);
      const squash = chatRoomOTSystem.squash([invertedDropCall, rejectCall]);

      for (const operation of squash) {
        operation.apply(stateSquashed);
      }

      expect(squash).toHaveLength(1);
      expect(stateNotSquashed).toEqual(stateSquashed);
    });

    it('Should squash calls, accept call should be inverted', () => {
      const stateNotSquashed = initState();
      const stateSquashed = initState();

      doApply(stateNotSquashed, stateSquashed, [initialCall, initialAcceptCall, acceptCall, dropCall]);
      const invertedAcceptCall = acceptCall.invert();
      const invertedDropCall = dropCall.invert();
      invertedDropCall.apply(stateNotSquashed);
      invertedAcceptCall.apply(stateNotSquashed);
      const squash = chatRoomOTSystem.squash([invertedDropCall, invertedAcceptCall]);

      for (const operation of squash) {
        operation.apply(stateSquashed);
      }

      expect(squash).toHaveLength(1);
      expect(stateNotSquashed).toEqual(stateSquashed);
    });
  });

  describe('HandleCallOperation and DropCallOperation squash', () => {
    const initialCall = CallOperation.create(initialCallInfo);
    const initialAcceptCall = HandleCallOperation.accept(PUB_KEY_1, null);
    const acceptCall = HandleCallOperation.accept(PUB_KEY_2, null);

    it('Should squash calls, accept call should not be inverted', () => {
      const stateNotSquashed = initState();
      const stateSquashed = initState();

      doApply(stateNotSquashed, stateSquashed, [initialCall, initialAcceptCall, acceptCall]);
      const rejectCall = HandleCallOperation.reject(PUB_KEY_3, null);
      const handled = new Map([[PUB_KEY_1, true], [PUB_KEY_2, true], [PUB_KEY_3, false]]);
      const dropCall = DropCallOperation.create(
        initialCallInfo.pubKey,
        initialCallInfo.peerId,
        initialCallInfo.timestamp,
        handled,200
      );
      rejectCall.apply(stateNotSquashed);
      dropCall.apply(stateNotSquashed);
      const squash = chatRoomOTSystem.squash([rejectCall, dropCall]);

      for (const operation of squash) {
        operation.apply(stateSquashed);
      }

      expect(squash).toHaveLength(1);
      expect(stateNotSquashed).toEqual(stateSquashed);
    });

    it('Should squash calls, accept call should be inverted', () => {
      const stateNotSquashed = initState();
      const stateSquashed = initState();

      doApply(stateNotSquashed, stateSquashed, [initialCall, initialAcceptCall, acceptCall]);
      const dropCall = DropCallOperation.create(
        initialCallInfo.pubKey,
        initialCallInfo.peerId,
        initialCallInfo.timestamp,
        new Map([[PUB_KEY_1, true]]),
        200
      );
      const invertedAcceptCall = acceptCall.invert();
      invertedAcceptCall.apply(stateNotSquashed);
      dropCall.apply(stateNotSquashed);
      const squash = chatRoomOTSystem.squash([invertedAcceptCall, dropCall]);

      for (const operation of squash) {
        operation.apply(stateSquashed);
      }

      expect(squash).toHaveLength(1);
      expect(stateNotSquashed).toEqual(stateSquashed);
    });
  });
});
