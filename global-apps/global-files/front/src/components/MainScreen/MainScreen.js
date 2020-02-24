import React, {useMemo, useState} from 'react';
import localforage from 'localforage';
import FSService from '../../modules/fs/FSService';
import GlobalFS from '../../common/GlobalFS';
import OfflineGlobalFS from '../../common/OfflineGlobalFS';
import ItemList from '../ItemList/ItemList';
import SideBar from '../SideBar/SideBar';
import Uploading from '../Uploading/Uploading';
import {connectService, AuthContext, checkAuth, RegisterDependency} from 'global-apps-common';
import Header from "../Header/Header";
import {withStyles} from "@material-ui/core";
import mainScreenStyles from "./mainScreenStyles";
import {withSnackbar} from "notistack";

function MainScreen({classes, publicKey}) {
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);

  const {fsService} = useMemo(() => {
    const globalFS = new GlobalFS();
    localforage.config({
      driver: localforage.INDEXEDDB,
      name: 'OperationsStore',
      storeName: 'OperationsStore'
    });
    const offlineGlobalFS = new OfflineGlobalFS(globalFS, localforage, window.navigator);
    offlineGlobalFS.init();
    const fsService = new FSService(offlineGlobalFS);

    return {fsService};
  }, [publicKey]);

  const openDrawer = () => {
    setIsDrawerOpen(true);
  };

  const closeDrawer = () => {
    setIsDrawerOpen(false);
  };

  return (
    <RegisterDependency name={FSService} value={fsService}>
      <div className={classes.root}>
        <Header openDrawer={openDrawer}/>
        <div className={classes.row}>
          <SideBar
            isDrawerOpen={isDrawerOpen}
            onDrawerClose={closeDrawer}
          />
          <ItemList/>
        </div>
      </div>
      <Uploading/>
    </RegisterDependency>
  );
}

export default withSnackbar(
  withStyles(mainScreenStyles)(
    connectService(AuthContext, ({publicKey}) =>
      ({publicKey})
    )(
      checkAuth(MainScreen)
    )
  )
);
