import EventEmitter from 'events';
import {retry, delay, RejectionError, CancelablePromise} from 'global-apps-common';
import {RETRY_TIMEOUT} from '../../common/utils';

class NotificationsService {
  constructor(url) {
    this._url = url;
    this._eventEmitter = new EventEmitter();
    this._initPromise = null;
    this._deleteRetry = {};
    this._mailboxes = new Set();
    this._order = 0;
    this._notifications = {};
  }

  static createFrom() {
    const url = '/notifications';

    return new NotificationsService(url);
  }

  waitNotification(mailbox, predicate) {
    let onNotification;
    return new CancelablePromise(resolve => {
      onNotification = function (notification) {
        if (predicate(notification)) {
          resolve(notification);
        }
      };
      this.addNotificationListener(mailbox, onNotification);
    })
      .finally(() => {
        this.removeNotificationListener(mailbox, onNotification);
      });
  }

  addNotificationListener(mailbox, listener) {
    this._eventEmitter.addListener(mailbox, listener);
    this._mailboxes.add(mailbox);

    if (this._mailboxes.size === 1 && this._eventEmitter.listenerCount(mailbox) === 1) {
      this._init();
    }
  }

  removeNotificationListener(mailbox, listener) {
    delete this._notifications[mailbox];
    this._eventEmitter.removeListener(mailbox, listener);

    if (!this._eventEmitter.listenerCount(mailbox)) {
      this._mailboxes.delete(mailbox);
      this._stopDeleteRetry(mailbox);
    }

    if (!this._mailboxes.size) {
      this._stop();
    }
  }

  send(publicKey, mailbox, data) {
    if (!publicKey || !mailbox || !data) {
      return;
    }

    return retry(async () => {
      await fetch(`${this._url}/send/${publicKey}/${encodeURIComponent(mailbox)}`, {
        method: 'POST',
        body: JSON.stringify(JSON.stringify({
          ...data,
          order: this._order++
        }))
      });
    }, RETRY_TIMEOUT);
  }

  _init() {
    this._initPromise = this._readAllMailboxes()
      .then(() => {
        if (!Object.keys(this._notifications).length) {
          return delay(RETRY_TIMEOUT);
        }
      })
      .then(() => {
        if (this._mailboxes.size) {
          this._init();
        }
      })
      .catch(error => {
        if (!(error instanceof RejectionError) && this._mailboxes.size) {
          this._init();
        }
      });
  }

  _readAllMailboxes() {
    const readMailboxPromises = [...this._mailboxes].map(mailbox => {
      return this._readMailbox(mailbox).catch(error => {
        if (!(error instanceof RejectionError)) {
          console.error(error);
        }
      });
    });

    return CancelablePromise.all(readMailboxPromises, () => {
      for (const readMailboxPromise of readMailboxPromises) {
        readMailboxPromise.cancel();
      }
    });
  }

  _readMailbox(mailbox) {
    let isCancelled = false;

    return new CancelablePromise(async (resolve, reject) => {
      (async () => {
        const data = await this._poll(mailbox);

        if (isCancelled) {
          resolve();
          return;
        }

        if (data === null) {
          if (this._notifications[mailbox] && this._notifications[mailbox].length) {
            const orderedNotifications = this._notifications[mailbox].sort((a, b) => a.order - b.order);

            for (const notification of orderedNotifications) {
              this._eventEmitter.emit(mailbox, notification);
            }

            delete this._notifications[mailbox];
          }

          resolve();
          return;
        }

        if (this._mailboxes.has(mailbox)) {
          this._notifications[mailbox] = this._notifications[mailbox] || [];
          this._notifications[mailbox].push(data.payload);
        }

        await this._delete(data.id, mailbox);
        resolve();
      })().catch(reject);
    }, () => {
      isCancelled = true;
    });
  }

  _stop() {
    if (this._initPromise) {
      this._initPromise.cancel();
      this._initPromise = null;
    }
  }

  _stopDeleteRetry(mailbox) {
    if (this._deleteRetry[mailbox]) {
      for (const retry of Object.values(this._deleteRetry[mailbox])) {
        retry.cancel();
      }

      delete this._deleteRetry[mailbox];
    }
  }

  _poll(mailbox) {
    return fetch(`${this._url}/poll/${encodeURIComponent(mailbox)}`)
      .then(res => res.json())
      .then(data => {
        if (data && data.payload) {
          data.payload = JSON.parse(data.payload);
        }

        return data;
      });
  }

  _delete(id, mailbox) {
    this._deleteRetry[mailbox] = this._deleteRetry[mailbox] || {};
    this._deleteRetry[mailbox][id] = retry(async () => {
      await fetch(`${this._url}/drop/${encodeURIComponent(mailbox)}/${id}`, {
        method: 'DELETE'
      });
    }, RETRY_TIMEOUT).then(() => {
      delete this._deleteRetry[mailbox][id];
    });

    return this._deleteRetry[mailbox][id];
  }
}

export default NotificationsService;
