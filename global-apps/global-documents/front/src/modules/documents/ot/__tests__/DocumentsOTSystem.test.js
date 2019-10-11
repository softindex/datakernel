import DocumentsOTSystem from '../DocumentsOTSystem';
import CreateOrDropDocuments from "../CreateOrDropDocuments";
import RenameDocument from "../RenameDocument";

describe('documentsOTSystem', () => {
  it('isEmpty', () => {
    expect(DocumentsOTSystem.isEmpty(new CreateOrDropDocuments({}))).toEqual(true);
    expect(DocumentsOTSystem.isEmpty(new RenameDocument({}))).toEqual(true);
    expect(DocumentsOTSystem.isEmpty(new RenameDocument({a: {prev: null, next: null}}))).toEqual(true);
    expect(DocumentsOTSystem.isEmpty(new RenameDocument({a: {prev: 'aa', next: 'aa'}}))).toEqual(true);
    expect(DocumentsOTSystem.isEmpty(new RenameDocument({a: {prev: 'aa', next: 'bb'}}))).toEqual(false);
    expect(DocumentsOTSystem.isEmpty(new CreateOrDropDocuments({
      a: {
        name: 'aa',
        participants: [],
        remove: true
      }
    }))).toEqual(true);
    expect(DocumentsOTSystem.isEmpty(new CreateOrDropDocuments({
      a: {
        name: 'aa',
        participants: ['a'],
        remove: true
      }
    }))).toEqual(false);
  });

  it('invert', () => {
    let state = new Map();

    const ops = [
      new CreateOrDropDocuments({
        a: {
          name: 'a',
          participants: ['a', 'b', 'c'],
          remove: false
        },
        b: {
          name: 'b',
          participants: ['a1', 'b1', 'c1'],
          remove: false
        }
      }),
      new RenameDocument({
        a: {
          prev: 'a',
          next: 'aa'
        },
        b: {
          prev: 'b',
          next: 'bb'
        }
      }),
      new CreateOrDropDocuments({
        b: {
          name: 'bb',
          participants: ['a1', 'b1', 'c1'],
          remove: true
        }
      })
    ];

    for (const op of ops) {
      state = op.apply(state);
    }

    expect(state).toEqual(new Map([['a', {name: 'aa', participants: ['a', 'b', 'c']}]]));

    const invertedOps = DocumentsOTSystem.invert(ops);
    for (const op of invertedOps) {
      state = op.apply(state);
    }
    expect(state.size).toEqual(0);
  });

  it('transform2Creates', () => {
    const initialState = [["b", {name: "b", participants: ["g", "f"]}]];

    const leftOps = [
      new CreateOrDropDocuments({
        a: {
          name: "a",
          participants: ["a", "b"],
          remove: false
        },
        b: {
          name: "b",
          participants: ["g", "f"],
          remove: true
        }
      })];

    const rightOps = [
      new CreateOrDropDocuments({
        b: {
          name: "b",
          participants: ["g", "f"],
          remove: true
        },
        c: {
          name: "c",
          participants: ["h", "t"],
          remove: false
        }
      })];

    const resultingState = [
      ["a", {name: "a", participants: ["a", "b"]}],
      ["c", {name: "c", participants: ["h", "t"]}]
    ];
    testTransform(initialState, leftOps, rightOps, resultingState);
  });

  it('transform2Renames', () => {
    const initialState = [["a", {name: "a", participants: ['a']}], ["b", {name: "b", participants: ['b']}]];

    const leftOps = [
      new RenameDocument({
        a: {
          prev: "a",
          next: "zz",
        },
        b: {
          prev: "b",
          next: "bb",
        }
      })];

    const rightOps = [
      new RenameDocument({
        a: {
          prev: "a",
          next: "aa"
        }
      })];
    const resultingState = [
      ["a", {name: "zz", participants: ['a']}],
      ["b", {name: "bb", participants: ['b']}]
    ];

    testTransform(initialState, leftOps, rightOps, resultingState);
  });

  it('transformRenameAndDrop', () => {
    const initialState = [["a", {name: "a", participants: ['a']}], ["b", {name: "b", participants: ['b']}]];

    const leftOps = [
      new RenameDocument({
        a: {
          prev: "a",
          next: "zz",
        },
        b: {
          prev: "b",
          next: "bb",
        }
      })];

    const rightOps = [
      new CreateOrDropDocuments({
        a: {
          name: "a",
          participants: ['a'],
          remove: true
        },
        c: {
          name: "c",
          participants: ["cc", "ccc"],
          remove: false
        }

      })];

    const resultingState = [
      ["b", {name: "bb", participants: ['b']}],
      ["c", {name: "c", participants: ["cc", "ccc"]}]
    ];
    testTransform(initialState, leftOps, rightOps, resultingState);
  });

  it('squash', () => {
    let stateFirst = new Map();
    let stateSecond = new Map();

    const ops = [
      new CreateOrDropDocuments({
        a: {
          name: "a",
          participants: ['a'],
          remove: false
        },
        b: {
          name: "b",
          participants: ['b'],
          remove: false
        },
        c: {
          name: "c",
          participants: ['c'],
          remove: false
        }
      }),
      new RenameDocument({
        a: {
          prev: 'a',
          next: 'aa'
        },
        b: {
          prev: 'b',
          next: 'bb'
        }
      }),
      new CreateOrDropDocuments({
        d: {
          name: "d",
          participants: ['d'],
          remove: false
        },
        b: {
          name: "bb",
          participants: ['b'],
          remove: true
        }
      }),
      new RenameDocument({
        d: {
          prev: 'd',
          next: 'dd'
        },
        c: {
          prev: 'c',
          next: 'cc'
        }
      })
    ];

    for (const op of ops) {
      stateFirst = op.apply(stateFirst);
    }

    const squashed = DocumentsOTSystem.squash(ops);

    for (const op of ops) {
      stateSecond = op.apply(stateSecond);
    }

    expect(stateFirst).toEqual(stateSecond);

    expect(squashed.length).toEqual(1);
    expect(squashed[0].documents).toEqual({
      a: {name: 'aa', participants: ['a'], remove: false},
      c: {name: 'cc', participants: ['c'], remove: false},
      d: {name: 'dd', participants: ['d'], remove: false}
    })
  })
});

function testTransform(initialState, leftOps, rightOps, resultingState) {
  let leftState = new Map(initialState);
  let rightState = new Map(initialState);

  for (const op of leftOps) {
    leftState = op.apply(leftState);
  }

  for (const op of rightOps) {
    rightState = op.apply(rightState);
  }

  expect(leftState).not.toEqual(rightState);

  const result = DocumentsOTSystem.transform(leftOps, rightOps);

  for (const op of result.leftOps) {
    leftState = op.apply(leftState);
  }

  for (const op of result.rightOps) {
    rightState = op.apply(rightState);
  }

  expect(rightState).toEqual(leftState);
  expect(rightState).toEqual(new Map(resultingState));

}
