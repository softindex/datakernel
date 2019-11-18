import {OTSystemBuilder, TransformResult} from 'ot-core';
import {isEqual, intersection} from 'lodash';
import MessageOperation from './MessageOperation';
import CallOperation from './CallOperation';
import DropCallOperation from './DropCallOperation';
import HandleCallOperation from './HandleCallOperation';

const chatRoomOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(MessageOperation, operation => operation.isEmpty())
  .withEmptyPredicate(CallOperation, operation => operation.isEmpty())
  .withEmptyPredicate(DropCallOperation, operation => operation.isEmpty())
  .withEmptyPredicate(HandleCallOperation, operation => operation.isEmpty())

  .withInvertFunction(MessageOperation, operation => operation.invert())
  .withInvertFunction(CallOperation, operation => operation.invert())
  .withInvertFunction(DropCallOperation, operation => operation.invert())
  .withInvertFunction(HandleCallOperation, operation => operation.invert())

  .withTransformFunction(MessageOperation, MessageOperation, (operationLeft, operationRight) => {
    return TransformResult.of([operationRight], [operationLeft]);
  })
  .withTransformFunction(MessageOperation, CallOperation, (operationLeft, operationRight) => {
    return TransformResult.of([operationRight], [operationLeft]);
  })
  .withTransformFunction(MessageOperation, DropCallOperation, (operationLeft, operationRight) => {
    return TransformResult.of([operationRight], [operationLeft]);
  })
  .withTransformFunction(MessageOperation, HandleCallOperation, (operationLeft, operationRight) => {
    return TransformResult.of([operationRight], [operationLeft]);
  })

  .withTransformFunction(CallOperation, CallOperation, (operationLeft, operationRight) => {
    if (!isEqual(operationLeft.getPrev(), operationRight.getPrev())) {
      throw new Error('Previous values should be equal');
    }

    const operationLeftNextValue = operationLeft.getNext();
    const operationRightNextValue = operationRight.getNext();

    if (isEqual(operationLeftNextValue, operationRightNextValue)) {
      return TransformResult.empty();
    }

    if (operationLeftNextValue === null) {
      return TransformResult.left([operationLeft.invert(), operationRight]);
    }

    if (operationRightNextValue === null) {
      return TransformResult.right([operationRight.invert(), operationLeft]);
    }

    if (operationLeftNextValue.timestamp >= operationRightNextValue.timestamp) {
      return TransformResult.right([operationRight.invert(), operationLeft]);
    } else {
      return TransformResult.left([operationLeft.invert(), operationRight]);
    }
  })
  .withTransformFunction(CallOperation, DropCallOperation, (operationLeft, operationRight) => {
    if (operationLeft.getNext() !== null) {
      return TransformResult.right([operationRight.invert(), operationLeft]);
    }

    if (!operationRight.isInvert()) {
      return TransformResult.left([operationLeft.invert(), operationRight]);
    }

    throw new Error('OTTransformException');
  })
  .withTransformFunction(CallOperation, HandleCallOperation, (operationLeft, operationRight) => {
    if (operationLeft.getNext() === null) {
      return TransformResult.right([operationRight.invert(), operationLeft]);
    }

    return TransformResult.of([operationRight], [operationLeft]);
  })

  .withTransformFunction(DropCallOperation, DropCallOperation, (operationLeft, operationRight) => {
    if (operationLeft.isEqual(operationRight)) {
      return TransformResult.empty();
    }

    if (!operationLeft.isInvert() && !operationRight.isInvert()) {
      if (operationLeft.getDropTime() <= operationRight.getDropTime()) {
        return TransformResult.right([operationRight.invert(), operationLeft]);
      } else {
        return TransformResult.left([operationLeft.invert(), operationRight]);
      }
    }

    throw new Error('OTTransformException');
  })
  .withTransformFunction(DropCallOperation, HandleCallOperation, (operationLeft, operationRight) => {
    if (!operationLeft.isInvert()) {
      const handled = operationLeft.getHandled();

      for (const [publicKey, status] of operationRight.getMapOperation().entries()) {
        if (status.next === null) {
          handled.delete(publicKey);
        } else {
          handled.set(publicKey, status.next);
        }
      }

      return TransformResult.right([DropCallOperation.create(
        operationLeft.getPubKey(),
        operationLeft.getPeerId(),
        operationLeft.getTimestamp(),
        handled,
        operationLeft.getDropTime()
      )])
    }

    throw new Error('OTTransformException');
  })

  .withTransformFunction(HandleCallOperation, HandleCallOperation, (operationLeft, operationRight) => {
    const mapOperationLeft = operationLeft.getMapOperation();
    const mapOperationRight = operationRight.getMapOperation();

    if (isEqual(mapOperationLeft, mapOperationRight)) {
      return TransformResult.empty();
    }

    const keysIntersection = intersection([...mapOperationLeft.keys()], [...mapOperationRight.keys()]);

    if (!keysIntersection.length) {
      return TransformResult.of([operationRight], [operationLeft]);
    }

    const rightTransformed = new Map();
    const leftTransformed = new Map();

    for (const [publicKey, status] of mapOperationLeft) {
      if (!keysIntersection.includes(publicKey)) {
        rightTransformed.set(publicKey, status);
      }
    }

    for (const [publicKey, status] of mapOperationRight) {
      if (!keysIntersection.includes(publicKey)) {
        leftTransformed.set(publicKey, status);
      }
    }

    for (const publicKey of keysIntersection) {
      const leftValue = mapOperationLeft.get(publicKey);
      const rightValue = mapOperationRight.get(publicKey);
      const leftValueNext = leftValue.next;
      const rightValueNext = rightValue.next;

      if (leftValueNext === null) {
        leftTransformed.set(publicKey, {
          prev: null,
          next: rightValueNext
        });
        continue;
      }

      if (rightValueNext === null) {
        rightTransformed.set(publicKey, {
          prev: null,
          next: leftValueNext
        });
        continue;
      }

      if (leftValueNext === rightValueNext) {
        continue;
      }

      if (leftValueNext === true) {
        rightTransformed.set(publicKey, {
          prev: rightValueNext,
          next: leftValueNext
        });
      } else {
        leftTransformed.set(publicKey, {
          prev: leftValueNext,
          next: rightValueNext
        });
      }
    }

    return TransformResult.of([new HandleCallOperation(leftTransformed)], [new HandleCallOperation(rightTransformed)]);
  })

  .withSquashFunction(MessageOperation, MessageOperation, doSquash((operationLeft, operationRight) => {
    if (operationRight.isEqual(operationLeft.invert())) {
      return MessageOperation.EMPTY;
    }

    return null;
  }))
  .withSquashFunction(CallOperation, CallOperation, doSquash((operationLeft, operationRight) => {
    if (operationLeft.getPrev() !== null && operationLeft.getNext() !== null && operationRight.getPrev() !== null &&
      operationRight.getNext() !== null) {
      return new CallOperation(operationLeft.getPrev(), operationRight.getNext());
    }

    return null;
  }))
  .withSquashFunction(DropCallOperation, DropCallOperation, doSquash((operationLeft, operationRight) => {
    if (operationLeft.isInversionFor(operationRight)) {
      return DropCallOperation.EMPTY;
    }

    return null;
  }))
  .withSquashFunction(HandleCallOperation, HandleCallOperation, (operationLeft, operationRight) => {
    const mapOperation = new Map();
    const mapOperationRight = operationRight.getMapOperation()

    for (const [publicKey, status] of operationLeft.getMapOperation().entries()) {
      const statusRight = mapOperationRight.get(publicKey);
      mapOperation.set(publicKey, {
        prev: status.prev,
        next: statusRight.next
      });
    }

    return new HandleCallOperation(mapOperation);
  })
  .withSquashFunction(DropCallOperation, HandleCallOperation, doSquash((operationLeft, operationRight) => {
    if (operationLeft.isInvert()) {
      const handled = operationLeft.getHandled();

      for (const [publicKey, status] of operationRight.getMapOperation().entries()) {
        if (status.next === null) {
          handled.delete(publicKey);
        } else {
          handled.set(publicKey, status.next);
        }
      }

      return new DropCallOperation(
        operationLeft.getPubKey(),
        operationLeft.getPeerId(),
        operationLeft.getTimestamp(),
        handled,
        operationLeft.getDropTime(),
        true
      );
    }

    return null;
  }))
  .withSquashFunction(HandleCallOperation, DropCallOperation, doSquash((operationLeft, operationRight) => {
    if (!operationRight.isInvert()) {
      const handled = operationRight.getHandled();

      for (const [publicKey, status] of operationLeft.getMapOperation().entries()) {
        if (status.next === null) {
          handled.set(publicKey, status.prev);
        } else {
          handled.delete(publicKey);
        }
      }

      return DropCallOperation.create(
        operationRight.getPubKey(),
        operationRight.getPeerId(),
        operationRight.getTimestamp(),
        handled,
        operationRight.getDropTime()
      );
    }

    return null;
  }))
  .build();

function doSquash(squashFn) {
  return (operationLeft, operationRight) => {
    if (operationLeft.isEmpty()) {
      return operationRight;
    }

    if (operationRight.isEmpty()) {
      return operationLeft;
    }

    return squashFn(operationLeft, operationRight);
  }
}

export default chatRoomOTSystem;
