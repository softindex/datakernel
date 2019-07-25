import createMapOtSystem from '../MapOTSystem'
import MapOTOperation from "../MapOTOperation";

const mapOtSystem = createMapOtSystem((left, right) => left.localeCompare(right));

describe('mapOTSystem', () => {
  it('isEmpty', () => {
    expect(mapOtSystem.isEmpty(new MapOTOperation({}))).toEqual(true);

    expect(mapOtSystem.isEmpty(new MapOTOperation({
      a: {
        prev: 'a',
        next: 'a'
      }
    }))).toEqual(true);

    expect(mapOtSystem.isEmpty(new MapOTOperation({
      a: {
        prev: 'a',
        next: 'a'
      },
      b: {
        prev: null,
        next: null
      }
    }))).toEqual(true);

    expect(mapOtSystem.isEmpty(new MapOTOperation({
      a: {
        prev: 'a',
        next: 'b'
      }
    }))).toEqual(false);

    expect(mapOtSystem.isEmpty(new MapOTOperation({
      a: {
        prev: null,
        next: 'b'
      }
    }))).toEqual(false);

    expect(mapOtSystem.isEmpty(new MapOTOperation({
      a: {
        prev: 'a',
        next: null
      }
    }))).toEqual(false);

  });

  it('invert', () => {
    let state = {};

    const ops = [
      new MapOTOperation({
        a: {
          prev: null,
          next: 'a'
        },
        b: {
          prev: null,
          next: 'b'
        }
      }),
      new MapOTOperation({
        a: {
          prev: 'a',
          next: 'aa'
        },
        c: {
          prev: null,
          next: 'c'
        },
        b: {
          prev: 'b',
          next: null
        }
      })
    ];

    for (const op of ops) {
      state = op.apply(state);
    }
    expect(state).toEqual({
      a: 'aa',
      c: 'c'
    });

    const invertedOps = mapOtSystem.invert(ops);
    console.log(invertedOps);
    for (const op of invertedOps) {
      state = op.apply(state);
    }
    expect(state).toEqual({});
  });

  it('transform', () => {
    let leftState = {};
    let rightState = {};

    const leftOps = [
      new MapOTOperation({
        a: {
          prev: null,
          next: 'a'
        },
        b: {
          prev: null,
          next: 'b'
        }
      }),
      new MapOTOperation({
        a: {
          prev: 'a',
          next: 'aa'
        },
        c: {
          prev: null,
          next: 'c'
        },
        b: {
          prev: 'b',
          next: null
        }
      })
    ];

    const rightOps = [
      new MapOTOperation({
        a: {
          prev: null,
          next: 'newValue'
        },
        b: {
          prev: null,
          next: 'bb'
        }
      }),
      new MapOTOperation({
        a: {
          prev: 'newValue',
          next: null
        },
        c: {
          prev: null,
          next: 'c'
        }
      })
    ];

    for (const op of leftOps) {
      leftState = op.apply(leftState);
    }

    for (const op of rightOps) {
      rightState = op.apply(rightState);
    }

    expect(leftState).not.toEqual(rightState);

    const result = mapOtSystem.transform(leftOps, rightOps);

    for (const op of result.leftOps) {
      leftState = op.apply(leftState);
    }

    for (const op of result.rightOps) {
      rightState = op.apply(rightState);
    }

    expect(rightState).toEqual(leftState);
    expect(rightState).not.toEqual({});
  });

  it('squash', () => {
    let stateFirst = {};
    let stateSecond = {};

    const ops = [
      new MapOTOperation({
        a: {
          prev: null,
          next: 'a'
        },
        b: {
          prev: null,
          next: 'b'
        }
      }),
      new MapOTOperation({
        a: {
          prev: 'a',
          next: 'aa'
        },
        c: {
          prev: null,
          next: 'c'
        },
        b: {
          prev: 'b',
          next: null
        }
      })
    ];

    for (const op of ops) {
      stateFirst = op.apply(stateFirst);
    }

    const squashed = mapOtSystem.squash(ops);

    for (const op of ops) {

      stateSecond = op.apply(stateSecond);
    }

    expect(stateFirst).toEqual(stateSecond);

    expect(squashed.length).toEqual(1);
    expect(squashed[0].values()).toEqual({
      a: {
        prev: null,
        next: 'aa'
      },
      c: {
        prev: null,
        next: 'c'
      }
    });
  })
});
