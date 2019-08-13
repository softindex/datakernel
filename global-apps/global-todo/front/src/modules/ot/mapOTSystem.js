import {OTSystemBuilder, TransformResult} from 'ot-core';
import MapOTOperation from './MapOTOperation';
import intersection from 'lodash/intersection';
import isEqual from 'lodash/isEqual';
import pickBy from 'lodash/pickBy';

const mapOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(MapOTOperation, operation => operation.isEmpty())
  .withInvertFunction(MapOTOperation, operation => operation.invert())
  .withSquashFunction(MapOTOperation, MapOTOperation, (firstOperation, secondOperation) => {
    const nextValues = {...firstOperation.getValues()};

    for (const [fieldName, {prev, next}] of Object.entries(secondOperation.getValues())) {
      if (!nextValues[fieldName]) {
        nextValues[fieldName] = {prev, next};
      } else {
        nextValues[fieldName] = {
          prev: nextValues[fieldName].prev,
          next
        };
      }
    }

    return new MapOTOperation(nextValues);
  })
  .withTransformFunction(MapOTOperation, MapOTOperation, (operationLeft, operationRight) => {
    const leftValues = operationLeft.getValues();
    const rightValues = operationRight.getValues();

    if (isEqual(leftValues, rightValues)) {
      return TransformResult.empty();
    }

    const conflictFields = intersection(Object.keys(leftValues), Object.keys(rightValues));

    if (!conflictFields.length) {
      return TransformResult.of([operationRight], [operationLeft]);
    }

    const rightTransformed = pickBy(leftValues, (val, key) => !conflictFields.includes(key));
    const leftTransformed = pickBy(rightValues, (val, key) => !conflictFields.includes(key));

    for (const key of conflictFields) {
      const left = leftValues[key];
      const right = rightValues[key];

      if (left.next === right.next) {
        continue;
      }

      if (left.next === null) {
        leftTransformed[key] = {
          prev: null,
          next: right.next
        };
        continue;
      }

      if (right.next === null) {
        rightTransformed[key] = {
          prev: null,
          next: left.next
        };
        continue;
      }

      if (left.next > right.next) {
        rightTransformed[key] = {
          prev: right.next,
          next: left.next
        };
      } else {
        leftTransformed[key] = {
          prev: left.next,
          next: right.next
        }
      }
    }

    return TransformResult.of(
      [new MapOTOperation(leftTransformed)],
      [new MapOTOperation(rightTransformed)]
    );
  })
  .build();

export default mapOTSystem;
