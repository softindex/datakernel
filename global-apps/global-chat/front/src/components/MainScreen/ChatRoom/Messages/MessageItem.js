import React from 'react';
import {withStyles} from '@material-ui/core';
import messagesStyles from './messagesStyles';
import Typography from '@material-ui/core/Typography';
import Paper from '@material-ui/core/Paper';
import classNames from 'classnames';
import DoneIcon from '@material-ui/icons/Done';
import DoneAllIcon from '@material-ui/icons/DoneAll';

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
          color="secondary"
          variant="subtitle2"
        >
          {author}
        </Typography>
        <Typography
          color="primary"
          variant="h6"
          gutterBottom
        >
          {text}
        </Typography>
        <Typography
          variant="caption">
          {time}
        </Typography>
      </Paper>
      {drawSide === 'left' && (
        <div className={classes.statusWrapper}>
          {loaded ? <DoneAllIcon/> : <DoneIcon/>}
        </div>
      )}
    </div>
  );
}

export default withStyles(messagesStyles)(MessageItem);