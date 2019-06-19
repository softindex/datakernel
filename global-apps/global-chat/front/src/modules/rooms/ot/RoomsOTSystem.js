import {OTSystemBuilder, TransformResult} from 'ot-core';
import RoomsOTOperation from './RoomsOTOperation';

const roomsOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(RoomsOTOperation, operation => operation.isEmpty())
  .withInvertFunction(RoomsOTOperation, operation => operation.invert())
  .withSquashFunction(RoomsOTOperation, RoomsOTOperation, (opA, opB) => {
    if (opA.isEmpty()) return opB;
    if (opB.isEmpty()) return opA;

    let roomsA = opA.rooms;
    let roomsB = opB.rooms;

    return new RoomsOTOperation(difference([...roomsA, ...roomsB], intersection(roomsA, roomsB)));
  })
  .withTransformFunction(RoomsOTOperation, RoomsOTOperation, (opLeft, opRight) => {
    let leftRooms = opLeft.rooms;
    let rightRooms = opRight.rooms;

    return TransformResult.of(
      [new RoomsOTOperation(difference(rightRooms, leftRooms))],
      [new RoomsOTOperation(difference(leftRooms, rightRooms))]
    );
  })
  .build();

const operation = (roomsA, roomsB, intersection) => roomsA.filter(a => intersection === roomsB.some(b => a.id === b.id));
const intersection = (roomsA, roomsB) => operation(roomsA, roomsB, true);
const difference = (original, other) => operation(original, other, false);

export default roomsOTSystem;
