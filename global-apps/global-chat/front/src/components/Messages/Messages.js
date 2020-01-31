import React, {useState} from 'react';
import {withSnackbar} from 'notistack';
import {withStyles} from '@material-ui/core';
import messagesStyles from './messagesStyles';
import MessageItem from "../MessageItem/MessageItem"
import CircularProgress from '@material-ui/core/CircularProgress';
import Paper from '@material-ui/core/Paper';
import Typography from '@material-ui/core/Typography';
import {getInstance, useService} from "global-apps-common";
import NamesService from "../../modules/names/NamesService";
import ChatRoomService from "../../modules/chatroom/ChatRoomService";
import ScrollArea from 'react-scrollbar';
import CallsService from '../../modules/calls/CallsService';
import CallButtons from '../CallButtons/CallButtons';

class MessagesView extends React.Component {
  scrollArea = React.createRef();

  componentDidUpdate(prevProps) {
    const currentScroll = this.scrollArea.current;
    if (currentScroll && this.props.messages.length !== prevProps.messages.length) {
      currentScroll.scrollYTo(currentScroll.state.realHeight);
    }
  }

  render() {
    const {
      classes,
      chatReady,
      namesReady,
      messages,
      publicKey,
      isHostValid,
      peerId,
      callerPeerId,
      calling,
      beingCalled,
      callLoading,
      onAccept,
      onDecline,
      onFinish
    } = this.props;

    return (
      <div className={classes.root}>
        {(!chatReady || !namesReady) && (
          <div className={classes.progressWrapper}>
            <CircularProgress/>
          </div>
        )}
        {chatReady && namesReady && (
          <ScrollArea
            speed={0.8}
            horizontal={false}
            ref={this.scrollArea}
            verticalContainerStyle={{background: 'transparent'}}
            verticalScrollbarStyle={{borderRadius: 4, width: 5}}
          >
            <div ref={this.wrapper} className={classes.wrapper}>
              {messages.map((message, index) => {
                const previousMessageAuthor = messages[index - 1] && messages[index - 1].authorPublicKey;
                let shape = 'start';
                if (previousMessageAuthor === message.authorPublicKey) {
                  shape = 'medium';
                }
                return !(message.authorPeerId && message.authorPeerId === callerPeerId && !isHostValid) && (
                  <MessageItem
                    key={index}
                    text={message.content}
                    author={
                      message.authorPublicKey === publicKey ? '' :
                        this.props.names.get(message.authorPublicKey)
                    }
                    time={new Date(message.timestamp).toLocaleString()}
                    loaded={message.loaded}
                    type={message.type}
                    drawSide={(message.authorPublicKey !== publicKey) ? 'left' : 'right'}
                    shape={shape}
                  />
                )
              })}
            </div>
          </ScrollArea>
        )}
        {beingCalled && (
          <div className={classes.fabWrapper}>
            <Paper elevation={0} className={classes.message}>
              <Typography
                className={classes.lightColor}
                variant="h6"
                gutterBottom
              >
                Accept or decline the call
              </Typography>
              <CallButtons
                showAccept={true}
                showClose={true}
                onAccept={onAccept}
                onClose={onDecline}
                callLoading={callLoading}
              />
            </Paper>
          </div>
        )}
        {calling && (
          <Paper elevation={0} className={classes.callingMessage}>
            <CallButtons
              showAccept={!peerId}
              showClose={peerId}
              onAccept={onAccept}
              onClose={onFinish}
              callLoading={callLoading}
            />
          </Paper>
        )}
      </div>
    )
  }
}

function Messages({classes, publicKey, enqueueSnackbar}) {
  const namesService = getInstance(NamesService);
  const {names, namesReady} = useService(namesService);
  const chatRoomService = getInstance(ChatRoomService);
  const {messages, chatReady, call, isHostValid} = useService(chatRoomService);
  const callsService = getInstance(CallsService);
  const {peerId} = useService(callsService);
  const calling = isHostValid && (call.callerInfo.publicKey === publicKey || call.handled.has(publicKey));
  const beingCalled = isHostValid && ![null, publicKey].includes(call.callerInfo.publicKey) &&
    !call.handled.has(publicKey);
  const [callLoading, setCallLoading] = useState(false);

  const props = {
    classes,
    publicKey,
    names,
    namesReady,
    messages,
    chatReady,
    isHostValid,
    peerId,
    callerPeerId: call.callerInfo.peerId,
    calling,
    beingCalled,
    callLoading,
    onAccept(event) {
      event.preventDefault();
      setCallLoading(true);

      chatRoomService.acceptCall()
        .catch(err => {
          chatRoomService.finishCall();
          enqueueSnackbar(err.message, {
            variant: 'error'
          });
        })
        .finally(() => {
          setCallLoading(false);
        });
    },
    onDecline(event) {
      event.preventDefault();
      setCallLoading(true);

      chatRoomService.declineCall()
        .catch(err => {
          enqueueSnackbar(err.message, {
            variant: 'error'
          });
        })
        .finally(() => {
          setCallLoading(false);
        });
    },
    onFinish(event) {
      event.preventDefault();
      setCallLoading(true);

      try {
        chatRoomService.finishCall();
      } catch (err) {
        enqueueSnackbar(err.message, {
          variant: 'error'
        });
      } finally {
        setCallLoading(false);
      }
    }
  };

  return <MessagesView {...props} />
}

export default withSnackbar(withStyles(messagesStyles)(Messages));
