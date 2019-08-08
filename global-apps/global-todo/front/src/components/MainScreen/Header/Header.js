import React from 'react';
import {ListItemIcon, withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import ArrowIcon from '@material-ui/icons/KeyboardArrowRight'
import headerStyles from './headerStyles';
import connectService from "../../../common/connectService";
import AccountContext from "../../../modules/account/AccountContext";
import MenuIcon from "@material-ui/icons/Menu";
import Drawer from "@material-ui/core/Drawer";
import List from "@material-ui/core/List";
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import Icon from '@material-ui/core/Icon';

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
              Global Todo List
            </Typography>
            <div className={classes.listTitleContainer}>
              {this.props.listName && (
                <>
                  <Typography
                    className={classes.listTitle}
                    color="inherit"
                  >
                    {this.props.listName}
                  </Typography>
                  <ListItemIcon className={classes.listItemIcon}>
                    <ArrowIcon className={classes.arrowIcon}/>
                  </ListItemIcon>
                </>
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
              <ListItem
                button
                onClick={this.props.logout}
              >
                <ListItemIcon>
                  <Icon className={classes.accountIcon}>logout</Icon>
                </ListItemIcon>
                <ListItemText primary={'Log Out'}/>
              </ListItem>
            </List>
          </div>
        </Drawer>
      </>
    );
  }
}

export default connectService(
  AccountContext,
  (state, accountService) => ({
    logout() {
      accountService.logout();
    }
  })
)(
  withStyles(headerStyles)(Header)
);
