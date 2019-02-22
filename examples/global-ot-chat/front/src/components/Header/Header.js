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

class Header extends React.Component {
  render() {
    return (
      <AppBar position="absolute" color="primary">
        <Toolbar>
          <Typography variant="h6" color="inherit">
            Global OT Chat
          </Typography>
          <div className={this.props.classes.grow}/>
          <IconButton
            color="inherit"
            onClick={this.props.onGraphToggle}
            disabled={!this.props.ready}
            className={this.props.classes.graphTriggerButton}
          >
            <TimelineIcon/>
          </IconButton>
        </Toolbar>
      </AppBar>
    );
  }
}

export default withStyles(headerStyles)(
  connectService(ChatContext, ({ready}) => ({ready}))(
    Header
  )
);
