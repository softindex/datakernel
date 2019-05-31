import editorOTSystem from '../editorOTSystem';
import InsertOperation from '../operations/InsertOperation';
import DeleteOperation from '../operations/DeleteOperation';

const INITIAL_STATE = 'abcdefghij';

describe('editorOTSystem', () => {
  it('insertsOnSamePosition', () => {
    test(new InsertOperation(2, '123'), new InsertOperation(2, '123'));
    test(new InsertOperation(2, '123'), new InsertOperation(2, '678'));
    test(new InsertOperation(2, '123'), new InsertOperation(2, '12345'));
    test(new InsertOperation(2, '123'), new InsertOperation(2, '67890'));
    test(new InsertOperation(2, '123'), new InsertOperation(2, '67890'));
  });

  it('insertsTotallyCross', () => {
    test(new InsertOperation(2, '1234'), new InsertOperation(3, '23'));
    test(new InsertOperation(2, '1234'), new InsertOperation(3, '67'));
    test(new InsertOperation(2, '1234'), new InsertOperation(2, '12'));
    test(new InsertOperation(2, '1234'), new InsertOperation(2, '67'));
    test(new InsertOperation(2, '1234'), new InsertOperation(4, '34'));
    test(new InsertOperation(2, '1234'), new InsertOperation(4, '67'));
  });

  it('insertsPartiallyCross', () => {
    test(new InsertOperation(2, '1234'), new InsertOperation(4, '3456'));
    test(new InsertOperation(2, '1234'), new InsertOperation(4, '6789'));
  });

  it('insertsDontCross', () => {
    test(new InsertOperation(2, '1234'), new InsertOperation(10, '1234'));
    test(new InsertOperation(2, '1234'), new InsertOperation(10, '6789'));
  });

  it('insertsSquash', () => {
    // Will overlap
    testSquash([new InsertOperation(2, '1234567')], new InsertOperation(2, '4567'), new InsertOperation(2, '123'));
    testSquash([new InsertOperation(3, '12qw34')], new InsertOperation(3, '1234'), new InsertOperation(5, 'qw'));
    testSquash([new InsertOperation(3, '12qwerty34')], new InsertOperation(3, '1234'), new InsertOperation(5, 'qwerty'));
    testSquash([new InsertOperation(3, '123qwerty4')], new InsertOperation(3, '1234'), new InsertOperation(6, 'qwerty'));
    testSquash([new InsertOperation(3, '1234qwerty')], new InsertOperation(3, '1234'), new InsertOperation(7, 'qwerty'));

    // Will not overlap
    testSquash(null, new InsertOperation(1, '123'), new InsertOperation(5, 'qw'));
    testSquash(null, new InsertOperation(10, '123'), new InsertOperation(5, 'qw'));
  });

  it('deletesOnSamePosition', () => {
    test(new DeleteOperation(1, 'bc'), new DeleteOperation(1, 'bc'));
    test(new DeleteOperation(1, 'bc'), new DeleteOperation(1, 'bcde'));
  });

  it('deletesTotallyCross', () => {
    test(new DeleteOperation(1, 'bcdef'), new DeleteOperation(2, 'cd'));
    test(new DeleteOperation(1, 'bcdef'), new DeleteOperation(2, 'cdef'));
  });

  it('deletesPartiallyCross', () => {
    test(new DeleteOperation(1, 'bcdef'), new DeleteOperation(3, 'defgh'));
  });

  it('deletesDontCross', () => {
    test(new DeleteOperation(1, 'bcd'), new DeleteOperation(6, 'gh'));
    test(new DeleteOperation(1, 'bcd'), new DeleteOperation(4, 'ef'));
  });

  it('deletesSquash', () => {
    // Will overlap
    testSquash([new DeleteOperation(1, 'bcdef')], new DeleteOperation(1, 'bcd'), new DeleteOperation(1, 'ef'));
    testSquash([new DeleteOperation(0, 'abcdef')], new DeleteOperation(1, 'bcd'), new DeleteOperation(0, 'aef'));
    testSquash([new DeleteOperation(2, 'cdef')], new DeleteOperation(2, 'cd'), new DeleteOperation(2, 'ef'));

    // Will not overlap
    testSquash(null, new DeleteOperation(1, 'bcd'), new DeleteOperation(2, 'fg'));
    testSquash(null, new DeleteOperation(2, 'cd'), new DeleteOperation(0, 'a'));
  });

  it('deleteAndInsertOnSamePosition', () => {
    test(new InsertOperation(1, '1234'), new DeleteOperation(1, 'bcde'));
    // test(new InsertOperation(1, '1234567'), new DeleteOperation(1, 'bcde'));
    // test(new InsertOperation(1, '1'), new DeleteOperation(1, 'bcde'));
  });

  it('deleteAndInsertTotallyCross', () => {
    test(new InsertOperation(1, '1234'), new DeleteOperation(2, 'cde'));
    test(new InsertOperation(1, '1234'), new DeleteOperation(2, 'c'));
    test(new InsertOperation(1, '1234'), new DeleteOperation(0, 'abcde'));
    test(new InsertOperation(1, '1234'), new DeleteOperation(0, 'abcdefg'));
  });

  it('deleteAndInsertPartiallyCross', () => {
    test(new InsertOperation(1, '1234'), new DeleteOperation(2, 'cdefgh'));
    test(new InsertOperation(3, '1234'), new DeleteOperation(0, 'abcde'));
  });

  it('deleteAndInsertDontCross', () => {
    test(new InsertOperation(1, '123'), new DeleteOperation(6, 'ghij'));
    test(new InsertOperation(1, '123'), new DeleteOperation(4, 'efgh'));
    test(new InsertOperation(3, '123'), new DeleteOperation(1, 'bc'));
    test(new InsertOperation(5, '123'), new DeleteOperation(1, 'bc'));
  });

  it('insertAndDeleteSquash', () => {
    // Totally same
    testSquash([], new InsertOperation(1, 'b123c'), new DeleteOperation(1, 'b123c'));

    // Delete overlaps insert
    testSquash([new DeleteOperation(1, 'bc')], new InsertOperation(2, '123'), new DeleteOperation(1, 'b123c'));
    testSquash([new DeleteOperation(1, '3c')], new InsertOperation(1, 'b12'), new DeleteOperation(1, 'b123c'));
    testSquash([new DeleteOperation(1, 'b')], new InsertOperation(2, '123c'), new DeleteOperation(1, 'b123c'));

    // Insert overlaps delete
    testSquash([new InsertOperation(2, '15')], new InsertOperation(2, '12345'), new DeleteOperation(3, '234'));
    testSquash([new InsertOperation(2, '5')], new InsertOperation(2, '12345'), new DeleteOperation(2, '1234'));
    testSquash([new InsertOperation(2, '1')], new InsertOperation(2, '12345'), new DeleteOperation(3, '2345'));
  });

  it('deleteAndInsertSquash', () => {
    // Totally same
    testSquash([], new DeleteOperation(1, 'bcd'), new InsertOperation(1, 'bcd'));

    testSquash([new InsertOperation(2, '123')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, '123cdefg'));
    testSquash([new InsertOperation(3, '123')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'c123defg'));
    testSquash([new InsertOperation(4, '123')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'cd123efg'));
    testSquash([new InsertOperation(5, '123')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'cde123fg'));
    testSquash([new InsertOperation(6, '123')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'cdef123g'));
    testSquash([new InsertOperation(7, '123')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'cdefg123'));

    testSquash([new InsertOperation(7, '123efg')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'cdefg123efg'));
    testSquash([new InsertOperation(5, '1cde')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'cde1cdefg'));
    testSquash([new InsertOperation(5, '123456cde')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'cde123456cdefg'));
    testSquash([new InsertOperation(5, '123456e')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'cde123456efg'));
    testSquash(null, new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'cde123456g'));
    testSquash(null, new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'c123456efg'));

    testSquash([new DeleteOperation(5, 'fg')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'cde'));
    testSquash([new DeleteOperation(3, 'defg')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'c'));
    testSquash([new DeleteOperation(2, 'cde')], new DeleteOperation(2, 'cdefg'), new InsertOperation(2, 'fg'));
  });
});

function test(left, right) {
  doTestTransformation(left, right);
  doTestTransformation(right, left);
}

function doTestTransformation(left, right) {
  let stateLeft = INITIAL_STATE;
  let stateRight = INITIAL_STATE;
  const result = editorOTSystem.transform([left], [right]);

  checkIfValid(left, stateLeft);
  stateLeft = left.apply(stateLeft);
  result.leftOps.forEach(editorOperation => {
    checkIfValid(editorOperation, stateLeft);
    stateLeft = editorOperation.apply(stateLeft);
  });

  checkIfValid(right, stateRight);
  stateRight = right.apply(stateRight);
  result.rightOps.forEach(editorOperation => {
    checkIfValid(editorOperation, stateRight);
    stateRight = editorOperation.apply(stateRight);
  });

  expect(stateLeft).toBe(stateRight);
}

function testSquash(expectedSquash, first, second) {
  const ops = [first, second];
  const actualSquash = editorOTSystem.squash(ops);
  if (expectedSquash == null) {
    expect(ops).toEqual(actualSquash);
  } else {
    expect(expectedSquash).toEqual(actualSquash);
  }
}

function checkIfValid(operation, builder) {
  if (operation instanceof DeleteOperation) {
    if (!operation.content === builder.substring(operation.position, operation.position + operation.content.length)) {
      throw new Error('Trying to delete non-present content');
    }
  }
}
