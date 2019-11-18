export default () => ({
  messages: new Set(),
  call: {
    callerInfo: {
      publicKey: null,
      peerId: null
    },
    timestamp: null,
    handled: new Map()
  }
});