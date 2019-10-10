import {OTSystemBuilder, TransformResult} from 'ot-core';
import RoomsOTOperation from './RoomsOTOperation';
import isEmpty from 'lodash/isEmpty';
import isEqual from 'lodash/isEqual';
import intersection from 'lodash/intersection';

const roomsOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(RoomsOTOperation, operation => operation.isEmpty())
  .withInvertFunction(RoomsOTOperation, operation => operation.invert())
  .withSquashFunction(RoomsOTOperation, RoomsOTOperation, (opA, opB) => {
    if (opA.isEmpty()) return opB;
    if (opB.isEmpty()) return opA;

    const roomsA = opA.rooms;
    const roomsB = opB.rooms;
    const conflictingKeys = intersection(Object.keys(roomsA), Object.keys(roomsB));

    return new RoomsOTOperation(Object.fromEntries(
      Object.entries(roomsA).concat(Object.entries(roomsB))
        .filter(([id, val]) => !conflictingKeys.contains(id))
    ));
  })
  .withTransformFunction(RoomsOTOperation, RoomsOTOperation, (opLeft, opRight) => {
    let leftRooms = opLeft.rooms;
    let rightRooms = opRight.rooms;

    const conflictingKeys = intersection(Object.keys(leftRooms), Object.keys(rightRooms));
    if (!conflictingKeys.length) {
      return TransformResult.of([opRight, opLeft]);
    }

    const leftTransformed = [];
    const rightTransformed = [];
    const leftDiffs = diff(rightRooms, conflictingKeys);
    const rightDiffs = diff(leftRooms, conflictingKeys);
    if (!isEmpty(leftDiffs)) leftTransformed.push(new RoomsOTOperation(leftDiffs));
    if (!isEmpty(rightDiffs)) rightTransformed.push(new RoomsOTOperation(rightDiffs));

    for (const key of conflictingKeys) {
      const left = leftRooms[key];
      const right = leftRooms[key];

      if (isEqual(left, right)) {
        continue;
      }

      if (left.remove || right.remove) {
        throw new Error("If any operation is 'remove', both operations should be equal");
      }

      // 2 adds with the same ID -> the one with more participants wins
      if (left.participants.length > right.participants.length) {
        rightTransformed.push(new RoomsOTOperation({[key]: {...right, remove: !right.remove}}));
        rightTransformed.push(new RoomsOTOperation({[key]: left}));
      } else {
        leftTransformed.push(new RoomsOTOperation({[key]: {...left, remove: !left.remove}}));
        leftTransformed.push(new RoomsOTOperation({[key]: right}));
      }
    }

    return TransformResult.of(leftTransformed, rightTransformed);
  })
  .build();

function diff(room, conflictingKeys) {
  return Object.fromEntries(Object.entries(room)
    .filter(([id, room]) => !conflictingKeys.contains(id)));
}

export default roomsOTSystem;
