import EventEmitter from 'events';
import {Service, randomString, delay, RejectionError, CancelablePromise} from 'global-apps-common';
import WebRTCPeer from '../../common/WebRTCPeer';
import {timeout} from '../../common/utils';
import * as messageTypes from '../notifications/types';
import {TimeoutError} from '../../common/utils';

function getDefaultState() {
  return {
    peerId: null,
    hostPeerId: null,
    streams: new Map()
  }
}

class CallsService extends Service {
  constructor(publicKey, notificationsService, createWebRTCPeer, waitAnswerTimeout, connectionTimeout) {
    super(getDefaultState());

    this._publicKey = publicKey;
    this._notificationsService = notificationsService;
    this._createWebRTCPeer = createWebRTCPeer;
    this._waitAnswerTimeout = waitAnswerTimeout;
    this._connectionTimeout = connectionTimeout;
    this._eventEmitter = new EventEmitter();
    this._waitAnswerPromise = null;
    this._peers = {};
    this._validationPeers = {};
    this._validationPromises = {};
    this._pendingNotificationPromises = new Set();
  }

  static createFrom(publicKey, notificationsService) {
    const createWebRTCPeer = () => {
      return new WebRTCPeer({
        iceServers: [{
          urls: 'stun:stun.l.google.com:19302'
        }]
      });
    };

    return new CallsService(publicKey, notificationsService, createWebRTCPeer, 60000, 10000);
  }

  addListener(event, listener) {
    this._eventEmitter.addListener(event, listener);
  }

  removeListener(event, listener) {
    this._eventEmitter.removeListener(event, listener);
  }

  stop() {
    for (const notification of this._pendingNotificationPromises) {
      notification.cancel();
    }

    for (const promise of Object.values(this._validationPromises)) {
      promise.cancel();
    }
  }

  async hostCall() {
    if (this.state.peerId) {
      this.finishCall();
    }

    const peerId = randomString(32);
    this.setState({
      peerId,
      hostPeerId: peerId,
      streams: new Map([[peerId, [await this._grabAudio()]]])
    });
    this._notificationsService.addNotificationListener(this._getMailbox(peerId), this._onNotification);
    this._waitAnswerPromise = delay(this._waitAnswerTimeout)
      .then(() => {
        this.finishCall();
      });
  }

  isValidHost(callerInfo) {
    const peerId = randomString(32);
    const validationMailbox = this._getMailbox(peerId);

    this._notificationsService.addNotificationListener(validationMailbox, this._onNotification);
    this._validationPeers[callerInfo.peerId] = this._createWebRTCPeer();
    this._validationPeers[callerInfo.peerId].onIceCandidate = candidate => {
      this._sendData(callerInfo, {
        candidate,
        peerId,
        type: messageTypes.VALIDATION_ICE_CANDIDATE
      });
    };

    return timeout(
      CancelablePromise.fromPromise(this._validationPeers[callerInfo.peerId].createOffer())
        .then(localDescription => {
          this._sendData(callerInfo, {
            localDescription,
            peerId,
            type: messageTypes.VALIDATION_OFFER
          });

          return this._notificationsService.waitNotification(validationMailbox, ({type, peerId}) => {
            return type === messageTypes.VALIDATION_ANSWER && peerId === callerInfo.peerId;
          });
        })
        .then(message => {
          return this._validationPeers[callerInfo.peerId].setRemoteDescription(message.localDescription);
        })
        .then(() => {
          return this._validationPeers[callerInfo.peerId].isSignallingStateStable();
        })
        .catch(error => {
          if (!(error instanceof RejectionError)) {
            console.warn(error);
            return false;
          }
        }),
      this._connectionTimeout
    )
      .catch(error => {
        if (error instanceof RejectionError || error instanceof TimeoutError) {
          return false;
        }

        throw error;
      })
      .finally(() => {
        this._notificationsService.removeNotificationListener(validationMailbox, this._onNotification);
        this._validationPeers[callerInfo.peerId].close();
        delete this._validationPeers[callerInfo.peerId];
      });
  }


  joinCall(callerInfo) {
    if (this.state.peerId) {
      this.finishCall();
    }

    const peerId = randomString(32);
    this.setState({
      peerId,
      hostPeerId: callerInfo.peerId
    });
    const mailbox = this._getMailbox(this.state.peerId);

    this._notificationsService.addNotificationListener(mailbox, this._onNotification);

    this._sendData(callerInfo, {
      peerId: this.state.peerId,
      type: messageTypes.GET_OFFER
    });

    return CancelablePromise.all([
      timeout(
        this._notificationsService.waitNotification(mailbox, ({type, peerId}) => {
          return type === messageTypes.OFFER && peerId === callerInfo.peerId
        }),
        this._connectionTimeout
      ),
      this._grabAudio()
    ])
      .then(([{localDescription}, mediaStream]) => {
        this.setState({
          streams: new Map([[this.state.peerId, [mediaStream]]])
        });

        this._peers[callerInfo.peerId] = this._createWebRTCPeer();

        this._peers[callerInfo.peerId].onIceCandidate = candidate => {
          this._sendData(callerInfo, {
            candidate,
            peerId: this.state.peerId,
            type: messageTypes.ICE_CANDIDATE
          });
        };

        this._peers[callerInfo.peerId].onClose = () => {
          this._peers[callerInfo.peerId].close();
          delete this._peers[callerInfo.peerId];

          if (!Object.keys(this._peers).length) {
            this._disconnectCall();
          }
        };

        this._peers[callerInfo.peerId].onTrack = ([newStream]) => {
          const streams = new Map(this.state.streams);
          streams.set(callerInfo.peerId, [...(streams.get(callerInfo.peerId) || []), newStream]);
          this.setState({streams});

          newStream.onremovetrack = () => {
            this.setState({
              streams: new Map([
                ...this.state.streams,
                [
                  callerInfo.peerId,
                  this.state.streams.get(callerInfo.peerId).filter(stream => stream !== newStream)
                ]
              ])
            });
          };
        };

        return this._peers[callerInfo.peerId].createAnswer(
          localDescription,
          new Map([[this.state.peerId, this.state.streams.get(this.state.peerId)]])
        );
      })
      .then(localDescription => {
        this._sendData(callerInfo, {
          localDescription,
          peerId: this.state.peerId,
          type: messageTypes.ANSWER
        });

        if (!this._peers[callerInfo.peerId].isSignallingStateStable()) {
          throw new Error('Signaling state is not stable');
        }
      })
      .catch(error => {
        this._disconnectCall();
        throw error;
      });
  }

  finishCall() {
    if (!this.state.peerId) {
      return;
    }

    this._eventEmitter.emit('finish', this.state.hostPeerId);
    this._clearState();
  }

  _disconnectCall() {
    this._eventEmitter.emit('disconnect', this.state.hostPeerId);
    this._clearState();
  }

  _clearState() {
    for (const peerId of Object.keys(this._peers)) {
      this._peers[peerId].close();
      delete this._peers[peerId];
    }

    this._notificationsService.removeNotificationListener(this._getMailbox(this.state.peerId), this._onNotification);
    this._clearWaitAnswerPromise();
    this.setState(getDefaultState());
    this._peers = {};
  }

  _onNotification = message => {
    (async () => {
      switch (message.type) {
        case messageTypes.VALIDATION_ICE_CANDIDATE:
        case messageTypes.VALIDATION_OFFER:
          if (!this._validationPeers[message.peerId]) {
            this._validationPeers[message.peerId] = this._createWebRTCPeer();
            this._validationPeers[message.peerId].onIceCandidate = candidate => {
              this._sendData(message, {
                candidate,
                peerId: this.state.peerId,
                type: messageTypes.VALIDATION_ICE_CANDIDATE
              });
            };

            this._validationPromises[message.peerId] = delay(this._connectionTimeout)
              .finally(() => {
                this._validationPeers[message.peerId].close();
                delete this._validationPeers[message.peerId];
                delete this._validationPromises[message.peerId];
              });
          }

          if (message.type === messageTypes.VALIDATION_ICE_CANDIDATE) {
            await this._validationPeers[message.peerId].addIceCandidate(message.candidate);
            return;
          }

          this._sendData(message, {
            localDescription: await this._validationPeers[message.peerId].createAnswer(message.localDescription),
            peerId: this.state.peerId,
            type: messageTypes.VALIDATION_ANSWER
          });
          break;
        case messageTypes.ICE_CANDIDATE:
          if (!this._peers[message.peerId]) {
            this._peers[message.peerId] = this._createWebRTCPeer();
          }

          await this._peers[message.peerId].addIceCandidate(message.candidate);
          break;
        case messageTypes.GET_OFFER:
          this._peers[message.peerId] = this._createWebRTCPeer();
          this._sendData(message, {
            localDescription: await this._createOfferWithAudio(message),
            peerId: this.state.peerId,
            type: messageTypes.OFFER
          });
          break;
        case messageTypes.TRACK_OFFER:
          await this._peers[message.peerId].setRemoteDescription(message.localDescription);
          this._sendData(message, {
            localDescription: await this._peers[message.peerId].createAndSetAnswer(),
            peerId: this.state.peerId,
            type: messageTypes.ANSWER
          });
          break;
        case messageTypes.ANSWER:
          await this._peers[message.peerId].setRemoteDescription(message.localDescription);
          this._clearWaitAnswerPromise();
          break;
        default:
          return;
      }
    })().catch(err => console.error(err));
  };

  async _createOfferWithAudio(callerInfo) {
    this._peers[callerInfo.peerId] = this._createWebRTCPeer();

    this._peers[callerInfo.peerId].onIceCandidate = candidate => {
      this._sendData(callerInfo, {
        candidate,
        peerId: this.state.peerId,
        type: messageTypes.ICE_CANDIDATE
      });
    };

    this._peers[callerInfo.peerId].onClose = async () => {
      const streams = new Map(this.state.streams);
      streams.delete(callerInfo.peerId);
      this.setState({
        streams
      });

      for (const peer of Object.values(this._peers)) {
        if (peer !== this._peers[callerInfo.peerId]) {
          peer.removeStream(callerInfo.peerId);
        }
      }

      this._peers[callerInfo.peerId].close();
      delete this._peers[callerInfo.peerId];

      if (!Object.keys(this._peers).length) {
        this.finishCall();
      }
    };

    this._peers[callerInfo.peerId].onTrack = stream => {
      const [newStream] = stream;
      const streams = new Map(this.state.streams);
      streams.set(callerInfo.peerId, [newStream]);
      this.setState({
        streams
      });

      for (const peer of Object.values(this._peers)) {
        if (peer !== this._peers[callerInfo.peerId]) {
          peer.addStream(callerInfo.peerId, newStream);
        }
      }
    };

    this._peers[callerInfo.peerId].onNegotiation = localDescription => {
      this._sendData(callerInfo, {
        localDescription,
        peerId: this.state.peerId,
        type: messageTypes.TRACK_OFFER
      });
    };

    return await this._peers[callerInfo.peerId].createOffer(this.state.streams);
  }

  async _grabAudio() {
    return await navigator.mediaDevices.getUserMedia({
      audio: true,
      video: false
    });
  }

  _clearWaitAnswerPromise() {
    if (this._waitAnswerPromise) {
      this._waitAnswerPromise.cancel();
      this._waitAnswerPromise = null;
    }
  }

  _getMailbox(peerId) {
    return `calls/${peerId}`;
  }

  _sendData(receiver, data) {
    const notification = this._notificationsService.send(receiver.publicKey, this._getMailbox(receiver.peerId), {
      ...data,
      publicKey: this._publicKey
    })
      .then(() => {
        this._pendingNotificationPromises.delete(notification);
      });
    this._pendingNotificationPromises.add(notification);
  }
}

export default CallsService;
