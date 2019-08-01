import React from 'react';
import {Paper, withStyles} from '@material-ui/core';
import emptyChatRoomStyles from './emptyChatRoomStyles';
import Typography from "@material-ui/core/Typography";

function EmptyChatRoom({classes}) {
  return (
    <div className={classes.root}>
      <div className={classes.headerPadding}/>
      <Paper className={classes.paper}>
        <Typography className={classes.typography} variant="h6">
          <div className={classes.startMessage}>
            Please select a chat to start messaging
          </div>
        </Typography>
      </Paper>
    </div>
  );
}

export default withStyles(emptyChatRoomStyles)(EmptyChatRoom);
