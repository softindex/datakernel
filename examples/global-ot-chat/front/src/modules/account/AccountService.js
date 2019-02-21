import Service from '../../common/Service';

class AccountService extends Service {
  constructor() {
    super({
      authorized: false,
      login: null
    });
  }

  async auth(login) {
    this.setState({
      login,
      authorized: true
    });
  }
}

export default AccountService;
