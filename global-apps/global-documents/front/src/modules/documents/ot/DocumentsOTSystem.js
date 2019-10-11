import {OTSystemBuilder, TransformResult} from 'ot-core';
import CreateOrDropDocuments from './CreateOrDropDocuments';
import RenameDocument from "./RenameDocument";
import isEqual from "lodash/isEqual";
import isEmpty from "lodash/isEmpty";
import pickBy from "lodash/pickBy";
import intersection from "lodash/intersection";
import omitBy from 'lodash/omitBy';
import transform from 'lodash/transform';

const documentsOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(CreateOrDropDocuments, operation => operation.isEmpty())
  .withEmptyPredicate(RenameDocument, operation => operation.isEmpty())

  .withInvertFunction(CreateOrDropDocuments, operation => operation.invert())
  .withInvertFunction(RenameDocument, operation => operation.invert())

  .withTransformFunction(CreateOrDropDocuments, CreateOrDropDocuments, (opLeft, opRight) => {
    let leftDocuments = opLeft.documents;
    let rightDocuments = opRight.documents;

    const conflictingKeys = intersection(Object.keys(leftDocuments), Object.keys(rightDocuments));
    if (!conflictingKeys.length) {
      return TransformResult.of([opRight, opLeft]);
    }

    const leftTransformed = [];
    const rightTransformed = [];
    const leftDiffs = omitBy(rightDocuments, (value, key) => conflictingKeys.includes(key));
    const rightDiffs = omitBy(leftDocuments, (value, key) => conflictingKeys.includes(key));
    if (!isEmpty(leftDiffs)) leftTransformed.push(new CreateOrDropDocuments(leftDiffs));
    if (!isEmpty(rightDiffs)) rightTransformed.push(new CreateOrDropDocuments(rightDiffs));

    for (const key of conflictingKeys) {
      const left = leftDocuments[key];
      const right = leftDocuments[key];

      if (isEqual(left, right)) {
        continue;
      }

      if (left.remove || right.remove) {
        throw new Error("If any operation is 'remove', both operations should be equal");
      }

      // 2 adds with the same ID -> the one with more participants wins
      if (left.participants.length > right.participants.length) {
        rightTransformed.push(new CreateOrDropDocuments({[key]: {...right, remove: !right.remove}}));
        rightTransformed.push(new CreateOrDropDocuments({[key]: left}));
      } else {
        leftTransformed.push(new CreateOrDropDocuments({[key]: {...left, remove: !left.remove}}));
        leftTransformed.push(new CreateOrDropDocuments({[key]: right}));
      }
    }

    return TransformResult.of(leftTransformed, rightTransformed);
  })
  .withTransformFunction(RenameDocument, RenameDocument, (operationLeft, operationRight) => {
    const leftRenames = operationLeft.renames;
    const rightRenames = operationRight.renames;

    if (isEqual(leftRenames, rightRenames)) {
      return TransformResult.empty();
    }

    const conflictFields = intersection(Object.keys(leftRenames), Object.keys(rightRenames));

    if (!conflictFields.length) {
      return TransformResult.of([operationRight], [operationLeft]);
    }

    const rightTransformed = pickBy(leftRenames, (val, key) => !conflictFields.includes(key));
    const leftTransformed = pickBy(rightRenames, (val, key) => !conflictFields.includes(key));

    for (const key of conflictFields) {
      const left = leftRenames[key];
      const right = rightRenames[key];

      if (left.next === right.next) {
        continue;
      }

      if (left.next.localeCompare(right.next) > 0) {
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
      [new RenameDocument(leftTransformed)],
      [new RenameDocument(rightTransformed)]
    );
  })
  .withTransformFunction(CreateOrDropDocuments, RenameDocument, (left, right) => {
    const documents = left.documents;
    const renames = right.renames;

    const conflictKeys = intersection(Object.keys(documents), Object.keys(renames));
    if (!conflictKeys.length) {
      return TransformResult.of([right, left]);
    }

    const leftRenames = pickBy(renames, (val, key) => !conflictKeys.includes(key));
    const rightDocuments = pickBy(documents, (val, key) => !conflictKeys.includes(key));
    for (const key of conflictKeys) {
      const {participants, remove} = documents[key];
      if (!remove) {
        throw new Error("Invalid operation");
      }
      rightDocuments[key] = {
        name: renames[key].next,
        participants,
        remove
      }
    }
    return TransformResult.of(
      [new RenameDocument(leftRenames)],
      [new CreateOrDropDocuments(rightDocuments)]
    );
  })

  .withSquashFunction(CreateOrDropDocuments, CreateOrDropDocuments, (opA, opB) => {
    if (opA.isEmpty()) return opB;
    if (opB.isEmpty()) return opA;

    const documentsA = opA.documents;
    const documentsB = opB.documents;
    const conflictingKeys = intersection(Object.keys(documentsA), Object.keys(documentsB));

    return new CreateOrDropDocuments({
      ...omitBy(documentsA, (value, key) => conflictingKeys.includes(key)),
      ...omitBy(documentsB, (value, key) => conflictingKeys.includes(key))
    });
  })
  .withSquashFunction(RenameDocument, RenameDocument, (firstOperation, secondOperation) => {
    const nextValues = {...firstOperation.renames};

    for (const [fieldName, {prev, next}] of Object.entries(secondOperation.renames)) {
      if (!nextValues[fieldName]) {
        nextValues[fieldName] = {prev, next};
      } else {
        nextValues[fieldName] = {
          prev: nextValues[fieldName].prev,
          next
        };
      }
    }

    return new RenameDocument(nextValues);
  })
  .withSquashFunction(CreateOrDropDocuments, RenameDocument, (first, second) => doSquash(second, first))
  .withSquashFunction(RenameDocument, CreateOrDropDocuments, (first, second) => doSquash(first, second))
  .build();

function doSquash(renameOp, createOrDropOp) {
  const renames = renameOp.renames;
  const documents = createOrDropOp.documents;
  if (!Object.keys(renames).every(value => documents.hasOwnProperty(value))) {
    return null;
  }

  return new CreateOrDropDocuments(transform(documents, (result, value, key) => {
    const rename = renames[key];
    if (rename) {
      result[key] = {...value, name: rename.next};
    } else {
      result[key] = value;
    }
  }, {}))
}

export default documentsOTSystem;
