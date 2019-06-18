import React from 'react';
import {withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import headerStyles from './headerStyles';
import Button from "@material-ui/core/Button";
import connectService from "../../common/connectService";
import AccountContext from "../../modules/account/AccountContext";

function Header ({classes, logout}) {
    return (
      <AppBar className={classes.appBar} position="fixed">
        <Toolbar>
          <Typography color="inherit" variant="h6" className={classes.title}>
            ChatApp
          </Typography>
          <Button color="inherit" className={classes.button} onClick={logout}>Log Out</Button>
        </Toolbar>
      </AppBar>
    );
}

export default connectService(
  AccountContext,
  (state, contactsService) => ({
    logout() {
      contactsService.logout();
    }
  })
)(
  withStyles(headerStyles)(Header)
);
