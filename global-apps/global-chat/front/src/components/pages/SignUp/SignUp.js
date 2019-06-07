import React from "react";
import connectService from "../../../common/connectService";
import AccountContext from "../../../modules/account/AccountContext";

class SignUp extends React.Component {
  constructor(props) {
    super(props);

    this.state = {privKey: ''};

    this._handleChange = this._handleChange.bind(this);
    this._handleSubmit = this._handleSubmit.bind(this);
  }

  _handleSubmit(e) {
    e.preventDefault();
    this.props.accountService.authByKey(this.state.privKey);
    this.props.history.push('/');
  }

  _handleChange(e) {
    this.setState({privKey: e.target.value})
  }

  render() {
    return <form onSubmit={this._handleSubmit}>
      <label>
        Private Key:
        <input type="text" name="name" onChange={this._handleChange}/>
      </label>
      <input type="submit" value="Submit"/>
    </form>
  }

}

export default connectService(AccountContext, (state, accountService) => ({accountService}))(SignUp);

