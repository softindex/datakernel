import EventEmitter from 'events';
import {RejectionError} from 'global-apps-common';

class CallsValidationService {
  constructor(callsService) {
    this._callsService = callsService;
    this._eventEmitter = new EventEmitter();
    this._validationPromise = null;
  }

  addChangeListener(listener) {
    this._eventEmitter.addListener('change', listener);
  }

  removeChangeListener(listener) {
    this._eventEmitter.removeListener('change', listener);
  }

  start(callerInfo) {
    this._validationPromise = this._callsService.isValidHost(callerInfo)
      .then(isHostValid => {
        this._eventEmitter.emit('change', isHostValid);
        this.start(callerInfo);
      })
      .catch(err => {
        if (err instanceof RejectionError) {
          return;
        }

        console.error(err);
      });
  }

  stop() {
    if (this._validationPromise) {
      this._validationPromise.cancel();
      this._validationPromise = null;
    }
  }
}

export default CallsValidationService;
