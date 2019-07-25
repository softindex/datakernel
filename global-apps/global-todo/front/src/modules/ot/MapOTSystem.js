import {OTSystemBuilder, TransformResult} from 'ot-core';
import MapOTOperation from './MapOTOperation';
import mergeWith from 'lodash/mergeWith';
import clone from 'lodash/clone';
import omitBy from 'lodash/omitBy';
import isEqual from 'lodash/isEqual';
import pickBy from 'lodash/pickBy';

const customizer = (first, second, $1, $2, $3, stack) => {
  if (stack.size === 0 && first && second && first.next === second.prev) {
    return {
      prev: first.prev,
      next: second.next
    };
  }
};

function filterEmpty(values) {
  return omitBy(values, ({prev, next}) => prev === next);
}

const createMapOTSystem = compareValues => new OTSystemBuilder()
  .withEmptyPredicate(MapOTOperation, operation => operation.isEmpty())
  .withInvertFunction(MapOTOperation, operation => operation.invert())
  .withSquashFunction(MapOTOperation, MapOTOperation, (operationFirst, operationSecond) => {
    const accumulator = clone(operationFirst.values());
    mergeWith(accumulator, operationSecond.values(), customizer);
    return new MapOTOperation(filterEmpty(accumulator));
  })
  .withTransformFunction(MapOTOperation, MapOTOperation, (operationLeft, operationRight) => {
    const leftValues = filterEmpty(clone(operationLeft.values()));
    const rightValues = filterEmpty(clone(operationRight.values()));

    if (isEqual(leftValues, rightValues)) {
      return TransformResult.empty();
    }

    const intersection = Object.keys(leftValues).filter({}.hasOwnProperty.bind(rightValues));

    if (!intersection.length) return TransformResult.of([operationRight], [operationLeft]);

    const rightTransformed = pickBy(leftValues, (val, key) => !intersection.includes(key));
    const leftTransformed = pickBy(rightValues, (val, key) => !intersection.includes(key));

    for (const key of intersection) {
      let left = leftValues[key];
      let right = rightValues[key];

      let leftNext = left.next;
      let rightNext = right.next;

      if (leftNext === rightNext) {
        break;
      }

      if (!leftNext) {
        leftTransformed[key] = right;
        break;
      }

      if (!rightNext) {
        rightTransformed[key] = left;
        break;
      }

      const result = compareValues(leftNext, rightNext);
      if (result > 0) {
        rightTransformed[key] = {
          prev: rightNext,
          next: leftNext
        };
      } else if (result < 0) {
        leftTransformed[key] = {
          prev: leftNext,
          next: rightNext
        }
      }
    }
    return TransformResult.of(
      [new MapOTOperation(leftTransformed)],
      [new MapOTOperation(rightTransformed)]
    );
  })
  .build();

export default createMapOTSystem;
