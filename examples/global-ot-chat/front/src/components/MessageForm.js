import React from 'react';
import connectService from '../common/connectService';
import ChatContext from '../modules/chat/ChatContext';

class MessageForm extends React.Component {
  state = {
    login: '',
    message: ''
  };

  onChangeField = (event) => {
    this.setState({
      [event.target.name]: event.target.value
    });
  };

  onSubmit = (event) => {
    event.preventDefault();
    this.props.sendMessage(this.state.login, this.state.message);
    this.setState({
      message: ''
    });
  };

  render() {
    return (
      <React.Fragment>
        <form onSubmit={this.onSubmit}>
          Login:
          <input
            type="text"
            value={this.state.login}
            name="login"
            onChange={this.onChangeField}
            required
          />
          <br/>
          <textarea
            value={this.state.message}
            name="message"
            onChange={this.onChangeField}
            required
          />
          <br/>
          <input type="submit"/>
        </form>
      </React.Fragment>
    );
  }
}

export default connectService(ChatContext, (state, chatService) => ({
  async sendMessage(login, message) {
    await chatService.sendMessage(login, message);
  }
}))(MessageForm);
