import React from 'react';
import Messages from './Messages';
import MessageForm from './MessageForm';
import ChatContext from '../modules/chat/ChatContext';

class App extends React.Component {
  componentDidMount() {
    this.props.chatService.init();
  }

  render() {
    return (
      <ChatContext.Provider value={this.props.chatService}>
        <Messages/>
        <MessageForm/>
      </ChatContext.Provider>
    );
  }
}

export default App;
