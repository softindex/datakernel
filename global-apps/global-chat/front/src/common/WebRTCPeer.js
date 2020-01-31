import 'webrtc-adapter';

class WebRTCPeer {
  constructor(configuration) {
    this.onIceCandidate = null;
    this.onClose = null;
    this.onTrack = null;
    this.onNegotiation = null;
    this._peerConnection = new RTCPeerConnection(configuration);
    this._pendingIceCandidates = [];
    this._senders = new Map();
    this._pendingAddStreams = new Map();
    this._pendingRemoveStreamIds = [];
    this._addTrackAvailable = true;
    this._generationId = 0;
  }

  async createOffer(streams) {
    this._setEventHandlers();
    this._peerConnection.onnegotiationneeded = async event => {
      if (event.target.connectionState === 'connected' && this.onNegotiation) {
        const localDescription = await this._createAndSetOffer();
        this.onNegotiation(localDescription);
      }
    };

    /**
     * Cannot create offer without media tracks or data channel
     */
    if (streams && streams.size) {
      for (const [id, streamById] of streams) {
        for (const stream of streamById) {
          this.addStream(id, stream);
        }
      }
    } else {
      this._peerConnection.createDataChannel('call');
    }

    return await this._createAndSetOffer();
  }

  async createAnswer(remoteDescription, streams) {
    this._setEventHandlers();
    await this.setRemoteDescription(remoteDescription);

    if (streams) {
      for (const [id, streamsById] of streams) {
        for (const stream of streamsById) {
          this.addStream(id, stream);
        }
      }
    }

    return await this.createAndSetAnswer();
  }

  async createAndSetAnswer() {
    const localDescription = await this._peerConnection.createAnswer();
    await this._peerConnection.setLocalDescription(localDescription);

    return {
      ...(localDescription.toJSON ? localDescription.toJSON() : localDescription),
      generationId: this._generationId
    };
  }

  async addIceCandidate(iceCandidate) {
    /**
     * Handle InvalidStateError exception when the RTCPeerConnection has no remote description.
     * Check the equality of candidate generation id and current peer generation id to handle candidates of
     * different generations
     */
    if (this._peerConnection.remoteDescription && iceCandidate.generationId === this._generationId) {
      await this._peerConnection.addIceCandidate(new RTCIceCandidate(iceCandidate));
    } else {
      this._pendingIceCandidates.push(iceCandidate);
    }
  }

  async setRemoteDescription(description) {
    if (!this._generationId || (description.type === 'offer' && description.generationId > this._generationId)) {
      this._generationId = description.generationId;
    }

    if (description.generationId !== this._generationId) {
      throw new Error('Local and remote descriptions have different generations');
    }

    await this._peerConnection.setRemoteDescription(new RTCSessionDescription(description));

    /**
     * Try to add ICE candidates which were added when peer had no remote description
     */
    if (this._pendingIceCandidates.length) {
      for (const iceCandidate of this._pendingIceCandidates) {
        try {
          await this._peerConnection.addIceCandidate(new RTCIceCandidate(iceCandidate));
        } catch (err) {
          console.warn(err);
        }
      }

      this._pendingIceCandidates = [];
    }

    /**
     * Add pending tracks to the peer to trigger negotiation because onconnectionstatechange event triggers only once
     * when connected
     */
    if (this._peerConnection.connectionState === 'connected') {
      this._addTrackAvailable = true;
      this._setPendingStreams();
    }
  }

  addStream(id, stream) {
    /**
     * The availability of adding tracks is 'true' at start and when the peer is connected and has remote description
     * Signaling state is 'stable' in two cases:
     * 1. Connection is new (local and remote descriptions are null).
     * 2. Negotiation was completed and connection has been established
     * Signaling state is 'have-remote-offer' when the remote peer created an offer, deliver it to the local peer,
     * which has set the offer as the remote description
     * No other signaling states are allowed to add track because negotiation will not be triggered
     */
    if (this._addTrackAvailable && ['have-remote-offer', 'stable'].includes(this._peerConnection.signalingState)) {
      for (const track of stream.getTracks()) {
        try {
          this._senders.set(id, this._peerConnection.addTrack(track, stream));
        } catch (err) {
          console.warn(err);
        }
      }
    } else {
      this._pendingAddStreams.set(id, stream);
    }
  }

  removeStream(id) {
    /**
     * The availability of removing tracks is 'true' at start and when the peer is connected and has remote description
     * Signaling state is 'stable' in two cases:
     * 1. Connection is new (local and remote descriptions are null).
     * 2. Negotiation was completed and connection has been established
     * Signaling state is 'have-remote-offer' when the remote peer created an offer, deliver it to the local peer,
     * which has set the offer as the remote description
     * No other signaling states are allowed to remove track because negotiation will not be triggered
     */
    if (this._addTrackAvailable && ['have-remote-offer', 'stable'].includes(this._peerConnection.signalingState)) {
      if (this._senders.get(id)) {
        this._peerConnection.removeTrack(this._senders.get(id));
      }
    } else {
      this._pendingRemoveStreamIds.push(id);
    }
  }

  isSignallingStateStable() {
    return this._peerConnection.signalingState === 'stable';
  }

  close() {
    this.onIceCandidate = null;
    this.onClose = null;
    this.onTrack = null;
    this.onNegotiation = null;
    this._peerConnection.close();
    this._peerConnection = null;
    this._pendingIceCandidates = [];
    this._senders = new Map();
    this._pendingAddStreams = new Map();
    this._pendingRemoveStreamIds = [];
    this._addTrackAvailable = true;
    this._generationId = 0;
  }

  _setEventHandlers() {
    this._peerConnection.ontrack = event => {
      if (event.streams && this.onTrack) {
        this.onTrack(event.streams);
      }
    };
    this._peerConnection.onicecandidate = event => {
      /**
       * Last candidate is always null, what indicates that ICE gathering has finished
       */
      if (event.candidate && this.onIceCandidate) {
        this.onIceCandidate({
          sdpMLineIndex: event.candidate.sdpMLineIndex,
          sdpMid: event.candidate.sdpMid,
          candidate: event.candidate.candidate,
          generationId: this._generationId
        });
      }
    };
    this._peerConnection.onconnectionstatechange = () => {
      /**
       * Add pending tracks to the peer when connected to trigger negotiation
       */
      if (this._peerConnection.connectionState === 'connected') {
        this._addTrackAvailable = true;
        this._setPendingStreams();
      }

      if (this.onClose && ['closed', 'disconnected', 'failed'].includes(this._peerConnection.connectionState)) {
        this.onClose();
      }
    };
    this._peerConnection.oniceconnectionstatechange = () => {
      if (this.onClose && ['closed', 'disconnected', 'failed'].includes(this._peerConnection.iceConnectionState)) {
        this.onClose();
      }
    };
  }

  async _createAndSetOffer() {
    this._addTrackAvailable = false;
    this._generationId++;
    const localDescription = await this._peerConnection.createOffer();
    await this._peerConnection.setLocalDescription(localDescription);

    return {
      ...(localDescription.toJSON ? localDescription.toJSON() : localDescription),
      generationId: this._generationId
    };
  }

  _setPendingStreams() {
    if (!this._pendingAddStreams.size && !this._pendingRemoveStreamIds.length) {
      return;
    }

    for (const [id, stream] of this._pendingAddStreams) {
      this.addStream(id, stream);
    }

    for (const id of this._pendingRemoveStreamIds) {
      this.removeStream(id);
    }

    this._pendingAddStreams = new Map();
    this._pendingRemoveStreamIds = [];
  }
}

export default WebRTCPeer;
