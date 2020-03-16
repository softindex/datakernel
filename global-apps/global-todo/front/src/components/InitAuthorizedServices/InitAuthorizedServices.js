import React, {useMemo} from 'react';
import {withSnackbar} from "notistack";
import ListService from "../../modules/list/ListService";
import {RegisterDependency, checkAuth, initService} from "global-apps-common";

function InitAuthorizedServices({enqueueSnackbar, children}) {
  const {listService} = useMemo(() => {
    const listService = ListService.create();
    return {
      listService
    }
  }, []);

  function errorHandler(err) {
    enqueueSnackbar(err.message, {
      variant: 'error'
    });
  }

  initService(listService, errorHandler);

  return (
    <RegisterDependency name={ListService} value={listService}>
      {children}
    </RegisterDependency>
  );
}

export default checkAuth(
  withSnackbar(InitAuthorizedServices)
);
