import React from 'react';
import localforage from 'localforage';
import checkAuth from '../HOC/checkAuth';
import FSService from '../../modules/fs/FSService';
import GlobalFS from '../../common/GlobalFS';
import OfflineGlobalFS from '../../common/OfflineGlobalFS';
import ItemList from '../ItemList';
import SideBar from '../SideBar';
import Uploading from '../Uploading';
import FSContext from '../../modules/fs/FSContext';
import AuthContext from '../../modules/auth/AuthContext';
import connectService from '../../common/connectService';
import Header from "../Header/Header";
import {withStyles} from "@material-ui/core";
import mainScreenStyles from "./mainScreenStyles";

class MainScreen extends React.Component {
  constructor(props) {
    super(props);

    const globalFS = new GlobalFS(props.publicKey);
    localforage.config({
      driver: localforage.INDEXEDDB,
      name: 'OperationsStore',
      storeName: 'OperationsStore'
    });
    const offlineGlobalFS = new OfflineGlobalFS(globalFS, localforage, window.navigator);
    offlineGlobalFS.init();
    this.fsService = new FSService(offlineGlobalFS);
    this.state = {
      isDrawerOpen: false
    };
  }

  openDrawer = () => {
    this.setState({
      isDrawerOpen: true
    });
  };

  closeDrawer = () => {
    this.setState({
      isDrawerOpen: false
    });
  };

  render() {
    return (
      <FSContext.Provider value={this.fsService}>
        <div className={this.props.classes.root}>
          <Header openDrawer={this.openDrawer}/>
          <div className={this.props.classes.row}>
            <SideBar
              isDrawerOpen={this.state.isDrawerOpen}
              onDrawerClose={this.closeDrawer}
            />
            <ItemList/>
          </div>
        </div>
        <Uploading/>
      </FSContext.Provider>
    );
  }
}

export default withStyles(mainScreenStyles)(
  connectService(AuthContext, ({publicKey}) => ({publicKey}))(checkAuth(MainScreen))
);
