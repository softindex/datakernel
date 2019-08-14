import React, {useState} from "react";
import List from "@material-ui/core/List";
import ListItem from "@material-ui/core/ListItem";
import {Icon, ListItemIcon, withStyles} from "@material-ui/core";
import AccountCircle from "@material-ui/icons/AccountCircle";
import ListItemText from "@material-ui/core/ListItemText";
import ChatIcon from "@material-ui/icons/Chat";
import drawerStyles from './drawerStyles';
import MUIDrawer from "@material-ui/core/Drawer";
import ProfileDialog from "../ProfileDialog/ProfileDialog";
import connectService from "../../common/connectService";
import AccountContext from "../../modules/account/AccountContext";
import CreateDocumentForm from "../CreateDocumentDialog/CreateDocumentDialog";

function Drawer({classes, open, onClose, logout, publicKey}) {
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
              <ListItemText primary="Create Document"/>
            </ListItem>
            <ListItem button onClick={logout}>
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
      <CreateDocumentForm
        open={showChatDialog}
        onClose={onChatDialogClose}
        publicKey={publicKey}
      />
    </>
  )
}

export default connectService(
  AccountContext,
  ({publicKey}, contactsService) => ({
    publicKey,
    logout() {
      contactsService.logout();
    }
  })
)(
  withStyles(drawerStyles)(Drawer)
);