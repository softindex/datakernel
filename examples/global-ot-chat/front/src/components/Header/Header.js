import React from 'react';
import {withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import TimelineIcon from '@material-ui/icons/Timeline';
import IconButton from '@material-ui/core/IconButton';
import headerStyles from './headerStyles';
import connectService from "../../common/connectService";
import ChatContext from "../../modules/chat/ChatContext";

function Header({onGraphToggle, ready, classes}) {
  return (
    <AppBar position="absolute" color="primary">
      <Toolbar>
        <Typography variant="h6" color="inherit">
          Global OT Chat
        </Typography>
        <div className={classes.grow}/>
        <IconButton
          color="inherit"
          onClick={onGraphToggle}
          disabled={!ready}
          className={classes.graphTriggerButton}
        >
          <TimelineIcon/>
        </IconButton>
      </Toolbar>
    </AppBar>
  );
}

export default withStyles(headerStyles)(
  connectService(ChatContext, ({ready}) => ({ready}))(
    Header
  )
);
