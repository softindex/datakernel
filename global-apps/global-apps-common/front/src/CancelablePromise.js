import {RejectionError} from './utils';

export class CancelablePromise {
  constructor(executor, cancel) {
    this._cancel = cancel;
    this._isResolved = false;
    this._isRejected = false;
    this._value = null;
    this._resolveHandlers = [];
    this._rejectHandlers = [];

    try {
      executor(this._resolve.bind(this), this._reject.bind(this));
    } catch (error) {
      this._reject(error);
    }
  }

  static resolve(value) {
    if (value instanceof CancelablePromise) {
      return value;
    }

    return new CancelablePromise(resolve => resolve(value));
  }

  static reject(value) {
    if (value instanceof CancelablePromise) {
      return value;
    }

    return new CancelablePromise((resolve, reject) => reject(value));
  }

  static fromPromise(promise) {
    if (promise instanceof CancelablePromise) {
      return promise;
    }

    return new CancelablePromise((resolve, reject) => {
      promise.then(resolve, reject);
    });
  }

  /**
   * @param {Array<CancelablePromise | Promise>} promises
   * @returns {CancelablePromise}
   */
  static all(promises) {
    const cancelablePromise = promises.map(CancelablePromise.fromPromise);
    const resolutions = [];
    let count = cancelablePromise.length;

    if (!count) {
      return CancelablePromise.resolve(resolutions);
    }

    const cancelPromises = () => {
      for (const promise of cancelablePromise) {
        if (promise.cancel) {
          promise.cancel();
        }
      }
    };

    return new CancelablePromise((resolve, reject) => {
      for (let i = 0; i < cancelablePromise.length; i++) {
        cancelablePromise[i]
          .then(value => {
            resolutions[i] = value;

            if (--count === 0) {
              resolve(resolutions);
            }
          })
          .catch(error => {
            reject(error);
            cancelPromises();
          });
      }
    }, cancelPromises);
  }

  cancel() {
    if (this._isResolved || this._isRejected) {
      return;
    }

    if (this._cancel) {
      this._cancel();
    }

    this._reject(new RejectionError());
  }

  then(onResolved, onRejected) {
    let promise;
    return new CancelablePromise((resolve, reject) => {
      this._onResult(
        onResolved ? value => {
          promise = this._onHandlerResult(onResolved, value, resolve, reject);
        } : resolve,
        onRejected ? error => {
          const rejected = error instanceof RejectionError;
          promise = this._onHandlerResult(onRejected, error, nextValue => {
            if (!rejected) {
              resolve(nextValue);
            }
          }, reject);

          if (rejected) {
            promise.cancel();
          }
        } : reject
      );
    }, () => {
      if (promise) {
        promise.cancel();
      }
      this.cancel();
    });
  }

  catch(handler) {
    return this.then(null, handler);
  }

  finally(handler) {
    let promise;
    return new CancelablePromise((resolve, reject) => {
      this._onResult(value => {
        promise = this._onHandlerResult(handler, undefined, () => resolve(value), reject);
      }, error => {
        promise = this._onHandlerResult(handler, undefined, () => reject(error), reject);
      });
    }, () => {
      if (promise) {
        promise.cancel();
      }
      this.cancel();
    });
  }

  _resolve(value) {
    setTimeout(() => {
      if (this._isResolved || this._isRejected) {
        return;
      }

      this._isResolved = true;
      this._value = value;

      for (const handler of this._resolveHandlers) {
        handler(value);
      }

      this._resolveHandlers = null;
      this._rejectHandlers = null;
    }, 0);
  }

  _reject(value) {
    setTimeout(() => {
      if (this._isResolved || this._isRejected) {
        return;
      }

      this._isRejected = true;
      this._value = value;

      if (!this._rejectHandlers.length && !(value instanceof RejectionError)) {
        console.error('Uncaught (in CancelablePromise)', value);
      }

      for (const handler of this._rejectHandlers) {
        handler(value);
      }

      this._resolveHandlers = null;
      this._rejectHandlers = null;
    }, 0);
  }

  _onResult(resolveHandler, rejectHandler) {
    if (this._isResolved) {
      resolveHandler(this._value);
      return;
    }

    if (this._isRejected) {
      rejectHandler(this._value);
      return;
    }

    this._resolveHandlers.push(resolveHandler);
    this._rejectHandlers.push(rejectHandler);
  }

  _onHandlerResult(handler, value, resolve, reject) {
    const promise = new CancelablePromise((resolve, reject) => {
      let result = handler(value);

      if (result instanceof Promise) {
        result = CancelablePromise.fromPromise(result);
      }

      if (result instanceof CancelablePromise) {
        result.then(resolve, reject);
      } else {
        resolve(result);
      }
    });
    promise._onResult(resolve, reject);
    return promise;
  }
}
