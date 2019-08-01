import {OTSystemBuilder, TransformResult} from 'ot-core';
import CreateOrDropDocument from './CreateOrDropDocument';
import RenameDocument from "./RenameDocument";

const documentsOTSystem = new OTSystemBuilder()
  .withEmptyPredicate(CreateOrDropDocument, operation => operation.isEmpty())
  .withEmptyPredicate(RenameDocument, operation => operation.isEmpty())

  .withInvertFunction(CreateOrDropDocument, operation => operation.invert())
  .withInvertFunction(RenameDocument, operation => operation.invert())

  .withTransformFunction(CreateOrDropDocument, CreateOrDropDocument, (left, right) => {
    return TransformResult.of([right], [left]);
  })
  .withTransformFunction(RenameDocument, RenameDocument, (left, right) => {
    if (left.id !== right.id) {
      return TransformResult.of([right], [left]);
    }

    const id = left.id;

    if (left.timestamp > right.timestamp)
      return TransformResult.right([new RenameDocument(id, right.next, left.next, left.timestamp)]);
    if (left.timestamp < right.timestamp)
      return TransformResult.left([new RenameDocument(id, left.next, right.next, right.timestamp)]);
    if (left.next > right.next)
      return TransformResult.right(new RenameDocument(id, right.next, left.next, left.timestamp));
    if (left.next < right.next)
      return TransformResult.left(new RenameDocument(id, left.next, right.next, right.timestamp));
    return TransformResult.empty();
  })
  .withTransformFunction(CreateOrDropDocument, RenameDocument, (left, right) => {
    if (left.id !== right.id) {
      return TransformResult.of([right], [left]);
    }

    const id = left.id;

    // Remove wins
    if (left.remove) {
      return TransformResult.right([new CreateOrDropDocument(id, right.next, left.participants)]);
    }

    return TransformResult.left([new RenameDocument(id, left.name, right.next, right.timestamp)]);
  })

  .withSquashFunction(CreateOrDropDocument, CreateOrDropDocument, (first, second) => {
    if (first.isEmpty()) return second;
    if (second.isEmpty()) return first;
    if (second.isEqual(first.invert())) return CreateOrDropDocument.EMPTY;
  })
  .withSquashFunction(RenameDocument, RenameDocument, (first, second) => {
    if (first.id !== second.id) {
      return null;
    }

    return new RenameDocument(first.id, first.prev, second.prev, second.timestamp);
  })
  .withSquashFunction(CreateOrDropDocument, RenameDocument, (first, second) => {
    if (first.id !== second.id) {
      return null;
    }

    return first.remove ?
      first :
      new CreateOrDropDocument(first.id, second.next, first.participants);
  })
  .withSquashFunction(RenameDocument, CreateOrDropDocument, (first, second) => {
    if (first.id !== second.id) {
      return null;
    }

    return second.remove ?
      new CreateOrDropDocument(first.id, first.prev, second.participants) :
      null;
  })
  .build();

export default documentsOTSystem;
