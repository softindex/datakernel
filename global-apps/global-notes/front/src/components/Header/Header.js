import React from 'react';
import {ListItemIcon, withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import ArrowIcon from '@material-ui/icons/KeyboardArrowRight';
import Icon from '@material-ui/core/Icon';
import headerStyles from './headerStyles';
import {connectService, AuthContext} from 'global-apps-common';

function Header({classes, title, onLogout}) {
  return (
    <AppBar className={classes.appBar} position="fixed">
      <Toolbar>
        <Typography
          color="inherit"
          variant="h6"
          className={classes.title}
        >
          Global Notes
        </Typography>
        <div className={classes.noteTitleContainer}>
          {title !== '' && (
            <ListItemIcon className={classes.listItemIcon}>
              <ArrowIcon className={classes.arrowIcon}/>
            </ListItemIcon>
          )}

              <Typography
                className={classes.noteTitle}
                color="inherit"
              >
                {title}
              </Typography>
        </div>
        <div
          color="inherit"
          onClick={onLogout}
          className={classes.logout}
        >
          <Icon className={classes.accountIcon}>logout</Icon>
        </div>
      </Toolbar>
    </AppBar>
  );
}

export default connectService(
  AuthContext,
  (state, accountService) => ({
    onLogout() {
      accountService.logout();
    }
  })
)(
  withStyles(headerStyles)(Header)
);
