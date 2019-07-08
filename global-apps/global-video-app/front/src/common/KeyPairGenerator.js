import EC from 'elliptic';

class KeyPairGenerator {
  constructor() {
    this._ec = new EC.ec('secp256k1');
  }

  generate() {
    const keys = this._ec.genKeyPair();
    const privateKey = keys.getPrivate().toString('hex');
    const publicKey = `${keys.getPublic().getX().toString('hex')}:${keys.getPublic().getY().toString('hex')}`;
    return {privateKey, publicKey};
  }
}

export default KeyPairGenerator;
