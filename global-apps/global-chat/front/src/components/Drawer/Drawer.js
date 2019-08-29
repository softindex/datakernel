import React, {useState} from "react";
import List from "@material-ui/core/List";
import ListItem from "@material-ui/core/ListItem";
import {Icon, ListItemIcon, withStyles} from "@material-ui/core";
import AccountCircle from "@material-ui/icons/AccountCircle";
import ListItemText from "@material-ui/core/ListItemText";
import ChatIcon from "@material-ui/icons/Chat";
import drawerStyles from './drawerStyles'
import MUIDrawer from "@material-ui/core/Drawer";
import ProfileDialog from "../ProfileDialog/ProfileDialog";
import {connectService} from "global-apps-common";
import {AuthContext} from "global-apps-common";
import CreateChatDialog from "../CreateChatDialog/CreateChatDialog";

function Drawer({classes, open, onClose, onLogout, publicKey}) {
  const [openProfile, setOpenProfile] = useState(false);
  const [showChatDialog, setShowChatDialog] = useState(false);

  function onProfileOpen() {
    setOpenProfile(true);
  }

  function onProfileClose() {
    setOpenProfile(false);
  }

  function onChatDialogShow() {
    setShowChatDialog(true);
    onClose();
  }

  function onChatDialogClose() {
    setShowChatDialog(false)
  }

  return (
    <>
      <MUIDrawer
        className={classes.drawer}
        open={open}
        onClose={onClose}
      >
        <div
          className={classes.list}
          role="presentation"
          onClick={onClose}
          onKeyDown={onClose}
        >
          <List>
            <ListItem button onClick={onProfileOpen}>
              <ListItemIcon>
                <AccountCircle className={classes.accountIcon}/>
              </ListItemIcon>
              <ListItemText primary="Profile"/>
            </ListItem>
            <ListItem button onClick={onChatDialogShow}>
              <ListItemIcon>
                <ChatIcon className={classes.accountIcon}/>
              </ListItemIcon>
              <ListItemText primary="New Chat"/>
            </ListItem>
            <ListItem button onClick={onLogout}>
              <ListItemIcon>
                <Icon className={classes.accountIcon}>logout</Icon>
              </ListItemIcon>
              <ListItemText primary="Log Out"/>
            </ListItem>
          </List>
        </div>
      </MUIDrawer>
      <ProfileDialog
        open={openProfile}
        onClose={onProfileClose}
        publicKey={publicKey}
      />
      <CreateChatDialog
        open={showChatDialog}
        onClose={onChatDialogClose}
        publicKey={publicKey}
      />
    </>
  )
}

export default connectService(
  AuthContext,
  ({publicKey}, authService) => ({
    publicKey,
    onLogout() {
      authService.logout();
    }
  })
)(
  withStyles(drawerStyles)(Drawer)
);

// function Drawer(props) {
//   const authService = getInstance(AuthService);
//   const {publicKey} = useService(authService);
//   const [openProfile, setOpenProfile] = useState(false);
//   const [showChatDialog, setShowChatDialog] = useState(false);
//
//   props = {
//     ...props,
//     publicKey,
//     openProfile,
//     showChatDialog,
//     onLogout() {
//       authService.onLogout();
//     },
//     onProfileOpen() {
//       setOpenProfile(true);
//     },
//     onProfileClose() {
//       setOpenProfile(false);
//     },
//     onChatDialogShow() {
//       setShowChatDialog(true);
//       props.onClose();
//     },
//     onChatDialogClose() {
//       setShowChatDialog(false)
//     }
//   };
//
//   return <DrawerView {...props}/>;
// }