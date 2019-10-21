import React from 'react';
import localforage from 'localforage';
import FSService from '../../modules/fs/FSService';
import GlobalFS from '../../common/GlobalFS';
import OfflineGlobalFS from '../../common/OfflineGlobalFS';
import ItemList from '../ItemList';
import SideBar from '../SideBar';
import Uploading from '../Uploading';
import FSContext from '../../modules/fs/FSContext';
import {connectService, AuthContext, checkAuth} from 'global-apps-common';
import Header from "../Header";
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



// import React, {useEffect, useState} from 'react';
// import localforage from 'localforage';
// import FSService from '../../modules/fs/FSService';
// import GlobalFS from '../../common/GlobalFS';
// import OfflineGlobalFS from '../../common/OfflineGlobalFS';
// import ItemList from '../ItemList';
// import SideBar from '../SideBar';
// import Uploading from '../Uploading';
// import FSContext from '../../modules/fs/FSContext';
// import {connectService, AuthContext, checkAuth} from 'global-apps-common';
// import Header from "../Header";
// import {withStyles} from "@material-ui/core";
// import mainScreenStyles from "./mainScreenStyles";
//
// function MainScreen({classes, publicKey}) {
//   const [isDrawerOpen, setIsDrawerOpen] = useState(false);
//   const globalFS = new GlobalFS(publicKey);
//   localforage.config({
//     driver: localforage.INDEXEDDB,
//     name: 'OperationsStore',
//     storeName: 'OperationsStore'
//   });
//   const offlineGlobalFS = new OfflineGlobalFS(globalFS, localforage, window.navigator);
//   offlineGlobalFS.init();
//   let fsService = new FSService(offlineGlobalFS);
//
//   const openDrawer = () => {
//     setIsDrawerOpen(true);
//   };
//
//   const closeDrawer = () => {
//     setIsDrawerOpen(false);
//   };
//
//   return (
//     <FSContext.Provider value={fsService}>
//       <div className={classes.root}>
//         <Header openDrawer={openDrawer}/>
//         <div className={classes.row}>
//           <SideBar
//             isDrawerOpen={isDrawerOpen}
//             onDrawerClose={closeDrawer}
//           />
//           <ItemList/>
//         </div>
//       </div>
//       <Uploading/>
//     </FSContext.Provider>
//   );
// }
//
// export default withStyles(mainScreenStyles)(
//   connectService(AuthContext, ({publicKey}) =>
//     ({publicKey})
//   )(
//     checkAuth(MainScreen)
//   )
// );
