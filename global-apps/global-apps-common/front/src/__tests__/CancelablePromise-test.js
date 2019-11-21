import {CancelablePromise, RejectionError} from '../CancelablePromise';

const PresenceError = new Error('Should not be here');

function wait(time, status = true) {
  return new CancelablePromise((resolve, reject) => {
    setTimeout(() => {
      status ? resolve() : reject();
    }, time);
  });
}

describe('CancelablePromise', () => {
  describe('resolve static method', () => {
    it('Should return new instance of CancelablePromise', () => {
      expect(CancelablePromise.resolve()).toBeInstanceOf(CancelablePromise);
    });

    it('Should resolve with correct result', async () => {
      const result = 'result';

      await CancelablePromise.resolve(result)
        .then(data => {
          expect(data).toEqual(result);
        });
    });
  });

  describe('reject static method', () => {
    it('Should return new instance of CancelablePromise', () => {
      expect(CancelablePromise.reject().catch(jest.fn)).toBeInstanceOf(CancelablePromise);
    });

    it('Should reject with correct error', async () => {
      const error = new Error('error');

      await CancelablePromise.reject(error)
        .catch(err => {
          expect(err).toEqual(error);
        });
    });
  });

  describe('fromPromise static method', () => {
    it('Should return unchanged promise if it is CancelablePromise', () => {
      const promise = new CancelablePromise(() => {});

      expect(CancelablePromise.fromPromise(promise)).toEqual(promise);
    });

    it('Should return new instance of CancelablePromise', () => {
      const promise = new Promise(() => {});

      expect(CancelablePromise.fromPromise(promise)).toBeInstanceOf(CancelablePromise);
    })
  });

  describe('all static method', () => {
    it('Should resolve with empty array if no promises passed', async () => {
      await CancelablePromise.all([])
        .then(values => {
          expect(values).toEqual([]);
        });
    });

    it('Should resolve with correct values', async () => {
      await CancelablePromise.all([
        CancelablePromise.resolve(1),
        CancelablePromise.resolve(2),
        CancelablePromise.resolve(3)
      ])
        .then(values => {
          expect(values).toEqual([1, 2, 3]);
        });
    });

    it('Should resolve with correct values in correct order', async () => {
      await CancelablePromise.all([
        new CancelablePromise(resolve => {
          wait(1000).then(() => resolve(1));
        }),
        new CancelablePromise(resolve => {
          wait(500).then(() => resolve(2));
        })
      ])
        .then(values => {
          expect(values).toEqual([1, 2]);
        });
    });

    it('Should reject if one of promises has rejected', async () => {
      const error = new Error('error');
      let caughtError = null;

      await CancelablePromise.all([
        new CancelablePromise((resolve, reject) => {
          wait(50, false).catch(() => reject(error));
        }),
        new CancelablePromise(resolve => {
          wait(50).then(() => resolve(2));
        })
      ])
        .catch(err => {
          caughtError = err;
        });

      expect(caughtError).toEqual(error);
    });
  });

  describe('cancel method', () => {
    it('Should call second argument passed to CancelablePromise', () => {
      const cancel = jest.fn();
      const promise = new CancelablePromise(() => {
        wait(1000);
      }, cancel);
      promise.cancel();

      expect(cancel).toBeCalled();
    });

    it('Should reject with RejectionError', async () => {
      const promise = new CancelablePromise(() => {
        // No resolve call
      });
      promise.cancel();

      let error;
      try {
        await promise;
      } catch (err) {
        error = err;
      }

      expect(error).toBeInstanceOf(RejectionError);
    });

    it('Should not reject if CancelablePromise has resolved', async () => {
      const result = 'result';
      const promise = new CancelablePromise(resolve => {
        resolve(result);
      })
        .then(data => {
          expect(data).toEqual(result);
        });

      try {
        await promise;
        promise.cancel()
      } catch {
        fail(PresenceError);
      }
    });
  });

  describe('then method', () => {
    it('Should resolve with correct result', async () => {
      const result = 'result';

      await new CancelablePromise(resolve => {
        resolve(result);
      })
        .then(data => {
          expect(data).toEqual(result);
        });
    });

    it('Should resolve with correct result in then chain', async () => {
      const result = 'result';

      await new CancelablePromise(resolve => {
        resolve(result)
      })
        .then(data => data)
        .then(data => data)
        .then(data => {
          expect(data).toEqual(result);
        });
    });

    it('Should resolve with correct result in then chain of CancelablePromises', async () => {
      const result = 'result';

      await new CancelablePromise(resolve => {
        resolve(result);
      })
        .then(data => {
          return new CancelablePromise(resolve => {
            resolve(data);
          });
        })
        .then(data => {
          return new CancelablePromise(resolve => {
            resolve(data);
          })
        })
        .then(data => {
          expect(data).toEqual(result);
        });
    });

    it('Should not reject if resolved', async () => {
      await new CancelablePromise(resolve => {
        resolve()
      })
        .catch(() => {
          fail(PresenceError);
        });
    });
  });

  describe('catch method', () => {
    it('Should reject with correct error', async () => {
      const error = new Error('error');

      await new CancelablePromise((resolve, reject) => {
        reject(error);
      })
        .catch(err => {
          expect(err).toEqual(error);
        })
    });

    it('Should reject with correct error in catch chain', async () => {
      const error = new Error('error');
      let caughtError = null;

      await new CancelablePromise((resolve, reject) => {
        reject(error);
      })
        .catch(err => {
          throw err;
        })
        .catch(err => {
          throw err;
        })
        .catch(err => {
          caughtError = err;
        });

      expect(caughtError).toEqual(error);
    });

    it('Should reject with correct error in catch chain of CancelablePromises', async () => {
      const error = new Error('error');
      let caughtError = null;

      await new CancelablePromise((resolve, reject) => {
        reject(error);
      })
        .catch(err => {
          return new CancelablePromise((resolve, reject) => {
            reject(err);
          });
        })
        .catch(err => {
          return new CancelablePromise((resolve, reject) => {
            reject(err);
          });
        })
        .catch(err => {
          caughtError = err;
        });

      expect(caughtError).toEqual(error);
    });

    it('Should not resolve if rejected', async () => {
      await new CancelablePromise((resolve, reject) => {
        reject();
      })
        .then(() => {
          fail(PresenceError);
        })
        .catch(jest.fn);
    });
  });

  describe('finally method', () => {
    it('Should call finally on resolve', async () => {
      const finallyFn = jest.fn();
      const result = 'result';

      await new CancelablePromise(resolve => {
        resolve(result);
      })
        .then(data => {
          expect(data).toEqual(result);
        })
        .finally(finallyFn);

      expect(finallyFn).toBeCalled();
    });

    it('Should call finally on reject', async () => {
      const finallyFn = jest.fn();
      const error = new Error('error');

      await new CancelablePromise((resolve, reject) => {
        reject(error);
      })
        .catch(err => {
          expect(err).toEqual(error);
        })
        .finally(finallyFn);

      expect(finallyFn).toBeCalled();
    });
  });
});
