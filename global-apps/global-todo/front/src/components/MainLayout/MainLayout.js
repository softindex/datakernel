import React from 'react';
import {withStyles} from '@material-ui/core';
import mainLayoutStyles from "./mainLayoutStyles";
import Container from '@material-ui/core/Container';
import Icon from '@material-ui/core/Icon';
import {connectService, AuthContext} from "global-apps-common";

function MainLayout({classes, logout, children}) {
  return (
    <>
      <div
        color="inherit"
        onClick={logout}
        className={classes.logout}
      >
        <Icon className={classes.logoutIcon} color="primary">logout</Icon>
      </div>
      <Container
        maxWidth="sm"
        className={classes.container}
      >
        {children}
      </Container>
    </>
  );
}

export default connectService(
  AuthContext, (state, accountService) => ({
    logout() {
      accountService.logout();
    }
  })
)(
  withStyles(mainLayoutStyles)(MainLayout)
);
