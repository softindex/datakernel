import React, {useMemo} from 'react';
import {withStyles} from '@material-ui/core';
import mainLayoutStyles from "./mainLayoutStyles";
import {withSnackbar} from "notistack";
import Container from '@material-ui/core/Container';
import ListService from "../../modules/list/ListService";
import Icon from '@material-ui/core/Icon';
import {RegisterDependency, checkAuth, connectService, AuthContext, initService} from "global-apps-common";

function MainLayout({classes, publicKey, logout, enqueueSnackbar, children}) {
  const {listService} = useMemo(() => {
    const listService = ListService.create();
    return {
      listService
    }
  }, [publicKey]);

  function errorHandler(err) {
    enqueueSnackbar(err.message, {
      variant: 'error'
    });
  }

  initService(listService, errorHandler);

  return (
    <RegisterDependency name={ListService} value={listService}>
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
    </RegisterDependency>
  );
}

export default checkAuth(
  connectService(
    AuthContext, ({publicKey}, accountService) => ({
      publicKey,
      logout() {
        accountService.logout();
      }
    })
  )(
    withSnackbar(
      withStyles(mainLayoutStyles)(MainLayout)
    )
  )
);
