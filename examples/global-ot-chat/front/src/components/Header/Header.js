import React from 'react';
import {withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import TimelineIcon from '@material-ui/icons/Timeline';
import IconButton from '@material-ui/core/IconButton';
import headerStyles from './headerStyles';

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
            onClick={this.props.onGraphToggle}
            color="inherit"
          >
            <TimelineIcon/>
          </IconButton>
        </Toolbar>
      </AppBar>
    );
  }
}

export default withStyles(headerStyles)(Header);
