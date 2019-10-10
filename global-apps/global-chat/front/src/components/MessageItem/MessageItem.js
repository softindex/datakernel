import React from 'react';
import {withStyles} from '@material-ui/core';
import messageItemStyles from './messageItemStyles';
import Typography from '@material-ui/core/Typography';
import Paper from '@material-ui/core/Paper';
import classNames from 'classnames';
import DoneIcon from '@material-ui/icons/Done';
import DoneAllIcon from '@material-ui/icons/DoneAll';

function MessageItem({text, author, time, drawSide, drawCaption, loaded, shape, classes}) {
  return (
    <div
      className={classNames(classes.messageRow, {
        [classes.messageRowRightAlign]: drawSide === 'right',
      })}
    >
      {drawCaption && (
        <Typography
          className={classes.timeCaption}
          color="textSecondary"
          variant="caption">
          {author ? author + ', ' : ''}{time}
        </Typography>
      )}
      <Paper
        elevation={0}
        className={classNames(classes.message, {
          [classes.messageFromOther]: drawSide === 'right',
          [classes.messageMedium]: shape === 'medium',
        })}
      >
        <Typography
          className={classes.messageText}
          variant="subtitle1"
        >
          {text}
        </Typography>
      </Paper>
      {/*{drawSide === 'right' && (*/}
      {/*  <div className={classes.statusWrapper}>*/}
      {/*    {loaded && <DoneAllIcon/>}*/}
      {/*    {!loaded && <DoneIcon/>}*/}
      {/*  </div>*/}
      {/*)}*/}
    </div>
  );
}

export default withStyles(messageItemStyles)(MessageItem);