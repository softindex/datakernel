import React from 'react';
import {ListItemIcon, withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import ArrowIcon from '@material-ui/icons/KeyboardArrowRight'
import headerStyles from './headerStyles';
import Profile from "../Profile/Profile";
import ProfileMenu from "../Profile/ProfileMenu/ProfileMenu";
import connectService from "../../common/connectService";
import AccountContext from "../../modules/account/AccountContext";
import RoomsContext from "../../modules/rooms/RoomsContext";
import MenuIcon from "@material-ui/icons/Menu";
import Drawer from "@material-ui/core/Drawer";
import List from "@material-ui/core/List";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import AccountCircle from "@material-ui/icons/AccountCircle";

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

  onOpenProfile = () => {
    this.setState({
      openProfile: true
    })
  };

  onCloseProfile = () => {
    this.setState({
      openProfile: false
    })
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
              <MenuIcon className={classes.menuIcon}/>
            </ListItemIcon>
            <Typography
              color="inherit"
              variant="h6"
              className={classes.title}
            >
              Global Chat
            </Typography>
            <div className={classes.chatTitleContainer}>
              {/*<ProfileMenu*/}
              {/*  className={classes.profileMenu}*/}
              {/*  onOpenProfile={this.onOpenProfile}*/}
              {/*  logout={this.props.logout}*/}
              {/*/>*/}
              {this.props.rooms.get(this.props.roomId) !== undefined && (
                <Typography
                  className={classes.chatTitle}
                  color="inherit"
                >
                  <ListItemIcon className={classes.listItemIcon}>
                    <ArrowIcon className={classes.arrowIcon}/>
                  </ListItemIcon>
                  {this.props.rooms.get(this.props.roomId).name}
                </Typography>
              )}
            </div>
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
              {['Profile', 'Log Out'].map((text) => (
                <ListItem
                  button
                  key={text}
                  onClick={text === 'Profile' ? this.onOpenProfile : this.props.logout}
                >
                  {text === 'Profile' && (
                    <ListItemIcon>
                      <AccountCircle className={classes.accountIcon}/>
                    </ListItemIcon>
                  )}
                  {text === 'Log Out' && (
                    <ListItemIcon>
                <span
                  className="iconify"
                  data-icon="mdi-logout"
                  data-inline="false"
                  style={{fontSize: 30}}
                />
                    </ListItemIcon>
                  )}
                  <ListItemText primary={text} />
                </ListItem>
              ))}
            </List>
          </div>
        </Drawer>
        <Profile
          open={this.state.openProfile}
          onClose={this.onCloseProfile}
          publicKey={this.props.publicKey}
        />
      </>
    );
  }
}

export default connectService(RoomsContext, (
  {ready, rooms}, roomsService) => ({ready, rooms, roomsService})
)(
  connectService(
    AccountContext,
    ({publicKey}, contactsService) => ({publicKey,
      logout() {
        contactsService.logout();
      }
    })
  )(
    withStyles(headerStyles)(Header)
  )
);
