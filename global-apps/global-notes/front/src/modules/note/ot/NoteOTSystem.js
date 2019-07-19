import assert from 'assert';
import {OTSystemBuilder, TransformResult} from 'ot-core';
import InsertOperation from './operations/InsertOperation';
import DeleteOperation from './operations/DeleteOperation';

const noteOTSystem = new OTSystemBuilder()
  .withTransformFunction(InsertOperation, InsertOperation, (left, right) => {
    if (left.position === right.position && left.content === right.content) {
      return TransformResult.empty();
    }

    if (
      (left.position === right.position && left.content < right.content)
      || left.position < right.position
    ) {
      return TransformResult.of([new InsertOperation(right.position + left.content.length, right.content)], [left]);
    }

    return TransformResult.of([right], [new InsertOperation(left.position + right.content.length, left.content)]);
  })
  .withTransformFunction(DeleteOperation, DeleteOperation, (left, right) => {
    // Left operation always has lesser position
    if (left.position > right.position) {
      const result = doTransformDeleteAndDelete(right, left);
      return TransformResult.of(result.rightOps, result.leftOps);
    } else {
      return doTransformDeleteAndDelete(left, right);
    }
  })
  .withTransformFunction(DeleteOperation, InsertOperation, (left, right) => {
    if (left.position === right.position) {
      return TransformResult.of([right], [
        new DeleteOperation(
          right.position + right.content.length,
          left.content.substring(0, left.content.length)
        )
      ]);
    }

    if (right.position > left.position && left.position + left.content.length > right.position) {
      const index = right.position - left.position;
      return TransformResult.right([
        new DeleteOperation(
          left.position,
          left.content.substring(0, index) + right.content + left.content.substring(index)
        )
      ]);
    }

    if (left.position > right.position) {
      return TransformResult.of([right], [new DeleteOperation(left.position + right.content.length, left.content)]);
    }

    return TransformResult.of([new InsertOperation(right.position - left.content.length, right.content)], [left]);
  })
  .withEmptyPredicate(InsertOperation, operation => operation.isEmpty())
  .withEmptyPredicate(DeleteOperation, operation => operation.isEmpty())
  .withInvertFunction(InsertOperation, operation => operation.invert())
  .withInvertFunction(DeleteOperation, operation => operation.invert())
  .withSquashFunction(InsertOperation, InsertOperation, (first, second) => {
    if (first.position <= second.position && first.position + first.content.length >= second.position) {
      return new InsertOperation(
        first.position,
        first.content.substring(0, second.position - first.position)
        + second.content
        + first.content.substring(second.position - first.position)
      );
    }
    return null;
  })
  .withSquashFunction(DeleteOperation, DeleteOperation, (first, second) => {
    // Second delete should overlap with first's position
    if (second.position <= first.position && second.position + second.content.length >= first.position) {
      return new DeleteOperation(
        second.position,
        second.content.substring(0, first.position - second.position)
        + first.content
        + second.content.substring(first.position - second.position)
      );
    }
    return null;
  })
  .withSquashFunction(DeleteOperation, InsertOperation, (first, second) => {
    // if positions match
    if (first.position === second.position) {
      if (second.content.length <= first.content.length) {
        if (first.content.startsWith(second.content)) {
          return new DeleteOperation(first.position + second.content.length,
            first.content.substring(second.content.length));
        }
        if (first.content.endsWith(second.content)) {
          return new DeleteOperation(first.position,
            first.content.substring(0, first.content.length - second.content.length));
        }

      } else {
        return getInsert(first, second);
      }
    }

    return null;
  })
  .withSquashFunction(InsertOperation, DeleteOperation, (first, second) => {
    // Operations cancel each other
    if (second.position === first.position && second.content === first.content) {
      // Empty operation
      return new InsertOperation(0, "");
    }

    // Delete totally overlaps insert
    if (second.position <= first.position &&
      second.position + second.content.length >= first.position &&
      second.position + second.content.length >= first.position + first.content.length) {
      return getDelete(first, second);
    }

    // Insert totally overlaps delete
    if (first.position <= second.position &&
      first.position + first.content.length >= second.position &&
      first.position + first.content.length >= second.position + second.content.length) {
      return getDelete(second, first).invert();
    }

    return null;
  })
  .build();

function doTransformDeleteAndDelete(left, right) {
  if (left.isEqual(right)) {
    return TransformResult.empty();
  }

  if (left.position === right.position) {
    if (left.content.length > right.content.length) {
      return TransformResult.right([
        new DeleteOperation(
          left.position,
          left.content.substring(left.content.length - right.content.length)
        )
      ]);
    } else {
      return TransformResult.left([
        new DeleteOperation(
          left.position,
          right.content.substring(right.content.length - left.content.length)
        )
      ]);
    }
  }

  if (left.position + left.content.length > right.position) {
    if (left.position + left.content.length >= right.position + right.content.length) {
      // Total crossing
      return TransformResult.right([
        new DeleteOperation(left.position,
          left.content.substring(0, right.position - left.position) +
          left.content.substring(right.position - left.position + right.content.length)
        )
      ]);
    }

    // Partial crossing
    return TransformResult.of([
      new DeleteOperation(left.position, right.content.substring(left.position + left.content.length - right.position))
    ], [
      new DeleteOperation(left.position, left.content.substring(0, right.position - left.position))
    ]);
  }

  // No crossing
  return TransformResult.of([new DeleteOperation(right.position - left.content.length, right.content)], [left]);
}

function getDelete(first, second) {
  return new DeleteOperation(
    second.position,
    second.content.substring(0, first.position - second.position)
    + second.content.substring(first.position - second.position + first.content.length)
  );
}

function getInsert(first, second) {
  assert(second.content.length > first.content.length);
  assert(first.position === second.position);

  const content = second.content;
  const subContent = first.content;
  const index = first.position;

  const startIndex = getStartIndex(subContent, content);
  const endIndex = getEndIndex(subContent, content);

  if (startIndex === -1 && content.length - subContent.length === endIndex) {
    return new InsertOperation(index, content.substring(0, endIndex));
  }

  if (subContent.length === startIndex) {
    return new InsertOperation(index + startIndex, content.substring(startIndex));
  }

  if (startIndex !== -1 && endIndex !== -1 && content.length - subContent.length >= endIndex - startIndex) {
    return new InsertOperation(
      index + startIndex,
      content.substring(startIndex, startIndex + content.length - subContent.length)
    );
  }

  return null;
}

function getStartIndex(subContent, content) {
  let startIndex = -1;
  for (let i = 0; i < subContent.length; i++) {
    if (content[i] === subContent[i]) {
      startIndex = i + 1;
    } else {
      break;
    }
  }
  return startIndex;
}

function getEndIndex(subContent, content) {
  let endIndex = -1;
  for (let i = content.length - 1, j = subContent.length - 1; i >= 0 && j >= 0; i--) {
    if (content[i] === subContent[j--]) {
      endIndex = i;
    } else {
      break;
    }
  }
  return endIndex;
}

export default noteOTSystem;
