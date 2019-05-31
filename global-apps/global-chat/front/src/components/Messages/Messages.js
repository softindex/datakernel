import React from 'react';
import connectService from '../../common/connectService';
import ChatRoomContext from '../../modules/chatroom/ChatRoomContext';
import AccountContext from '../../modules/account/AccountContext';
import {withStyles} from '@material-ui/core';
import messagesStyles from './messagesStyles';
import Typography from '@material-ui/core/Typography';
import Paper from '@material-ui/core/Paper';
import DoneIcon from '@material-ui/icons/Done';
import DoneAllIcon from '@material-ui/icons/DoneAll';
import classNames from 'classnames';
import CircularProgress from '@material-ui/core/CircularProgress';
import Grow from '@material-ui/core/Grow';

class Messages extends React.Component {
  wrapper = React.createRef();

  componentDidUpdate(prevProps) {
    if (this.props.messages.length !== prevProps.messages.length) {
      this.wrapper.current.scrollTop = this.wrapper.current.scrollHeight;
    }
  }

  render() {
    return (
      <div className={this.props.classes.root}>
        <Grow in={!this.props.ready}>
          <div className={this.props.classes.progressWrapper}>
            <CircularProgress/>
          </div>
        </Grow>
        <div ref={this.wrapper} className={this.props.classes.wrapper}>
          {this.props.messages.map((message, index) => {
            const previousMessageAuthor = this.props.messages[index - 1] && this.props.messages[index - 1].author;
            let shape = 'start';
            if (previousMessageAuthor === message.author) {
              shape = 'medium';
            }
            return (
              <MessageItem
                key={index}
                text={message.content}
                author={message.author}
                time={new Date(message.timestamp).toLocaleString()}
                loaded={message.loaded}
                classes={this.props.classes}
                drawSide={(message.author === this.props.login) ? 'left' : 'right'}
                shape={shape}
              />
            )
          })}
        </div>
      </div>
    )
  }
}

function MessageItem({text, author, time, drawSide, loaded, shape, classes}) {
  return (
    <div
      className={classNames(classes.messageRow, {
        [classes.messageRowRightAlign]: drawSide === 'right',
      })}
    >
      <Paper
        elevation={0}
        className={classNames(classes.message, {
          [classes.messageFromOther]: drawSide === 'right',
          [classes.messageMedium]: shape === 'medium',
        })}
      >
        <Typography
          color="textSecondary"
          variant="subtitle2"
        >
          {author}
        </Typography>
        <Typography
          color="textPrimary"
          variant="h6"
          gutterBottom
        >
          {text}
        </Typography>
        <Typography
          color="textSecondary"
          variant="caption">
          {time}
        </Typography>
      </Paper>
      {/*drawSide === 'left' && (
        <div className={classes.statusWrapper}>
          {loaded ? <DoneAllIcon/> : <DoneIcon/>}
        </div>
      )*/}
    </div>
  );
}

export default withStyles(messagesStyles)(Messages);
