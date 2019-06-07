import Service from '../../common/Service';

class AccountService extends Service {
  constructor(cookies) {
    super({
      authorized: false,
      privateKey: null
    });
    this._cookies = cookies;
  }

  init() {
    const privateKey = this._cookies.get('Key');
    if (privateKey) {
      this.setState({
        authorized: true,
        privateKey
      });
    }
  }

  authByKey(privateKey){
    this._cookies.set('Key', privateKey);
    this.setState({
      authorized: true,
      privateKey
    })
  }

  logout() {
    this._cookies.remove('Key');

    this.setState({
      authorized: false,
      privateKey: null
    });
  }
}

export default AccountService;
