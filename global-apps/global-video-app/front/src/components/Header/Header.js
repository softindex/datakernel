import React from 'react';
import {ListItemIcon, withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import headerStyles from './headerStyles';
import connectService from "../../common/connectService";
import AuthContext from "../../modules/auth/AuthContext";
import MenuIcon from "@material-ui/icons/Menu";
import Drawer from "@material-ui/core/Drawer";
import List from "@material-ui/core/List";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import CloudUpload from "@material-ui/icons/CloudUpload";
import VideoLibrary from "@material-ui/icons/VideoLibrary";
import ExitToApp from "@material-ui/icons/ExitToApp";

class Header extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      openProfile: false,
      openDrawer: false
    };
  }

  onOpenDrawer = () => {
    this.setState({
      openDrawer: true
    });
  };

  onCloseDrawer = () => {
    this.setState({
      openDrawer: false
    });
  };

  goto = path => {
    this.props.history.push(path);
  };

  render() {
    const {classes} = this.props;
    return (
      <>
        <AppBar className={classes.appBar} position="fixed">
          <Toolbar>
            <ListItemIcon
              aria-label="Open drawer"
              onClick={this.onOpenDrawer}
              edge="start"
              className={classes.iconButton}
            >
              <MenuIcon/>
            </ListItemIcon>
            <Typography
              color="inherit"
              variant="h6"
              className={classes.title}
            >
              Global Videos
            </Typography>
          </Toolbar>
        </AppBar>
        <Drawer
          className={classes.drawer}
          open={this.state.openDrawer}
          onClose={this.onCloseDrawer}
        >
          <div
            className={classes.list}
            role="presentation"
            onClick={this.onCloseDrawer}
            onKeyDown={this.onCloseDrawer}
          >
            <List>
              {['My Videos', 'Upload', 'Log Out'].map((text) => (
                <ListItem
                  button
                  key={text}
                  onClick={() => text === 'My Videos' ?
                    this.goto('/') :
                    text === 'Upload' ?
                      this.goto('/upload') :
                      this.props.logout()}
                >
                  <ListItemIcon className={classes.menuItemIcon}>
                    {text === 'My Videos' ?
                      <VideoLibrary/> :
                      text === 'Upload' ?
                        <CloudUpload/> :
                        <ExitToApp/>
                    }
                  </ListItemIcon>
                  <ListItemText primary={text}/>
                </ListItem>
              ))}
            </List>
          </div>
        </Drawer>
      </>
    );
  }
}

export default connectService(
  AuthContext,
  ({publicKey}, contactsService) => ({
    publicKey,
    logout() {
      contactsService.logout();
    }
  })
)(
  withStyles(headerStyles)(Header)
);
