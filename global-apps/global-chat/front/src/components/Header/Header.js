import React, {useState} from 'react';
import {ListItemIcon, withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import ArrowIcon from '@material-ui/icons/KeyboardArrowRight'
import headerStyles from './headerStyles';
import connectService from "../../common/connectService";
import RoomsContext from "../../modules/rooms/RoomsContext";
import MenuIcon from "@material-ui/icons/Menu";
import Drawer from "../Drawer/Drawer";
import {getRoomName} from "../../common/utils";
import ContactsContext from "../../modules/contacts/ContactsContext";
import MyProfileContext from "../../modules/myProfile/MyProfileContext";

function Header({classes, rooms, roomId, names, publicKey, profile}) {
  const [openDrawer, setOpenDrawer] = useState(false);

  function onDrawerOpen() {
    setOpenDrawer(true);
  }

  function onDrawerClose() {
    setOpenDrawer(false);
  }

  return (
    <>
      <AppBar className={classes.appBar} position="fixed">
        <Toolbar>
          <ListItemIcon
            onClick={onDrawerOpen}
            edge="start"
            className={classes.iconButton}
          >
            <MenuIcon/>
          </ListItemIcon>
          <Typography
            color="inherit"
            variant="h6"
            className={classes.title}
          >
            Global Chat
          </Typography>
          <div className={classes.chatTitleContainer}>
            {rooms.has(roomId) && (
              <Typography
                className={classes.chatTitle}
                color="inherit"
              >
                <ListItemIcon className={classes.listItemIcon}>
                  <ArrowIcon className={classes.arrowIcon}/>
                </ListItemIcon>
                {getRoomName(rooms.get(roomId).participants, names, publicKey, profile.name)}
              </Typography>
            )}
          </div>
        </Toolbar>
      </AppBar>
      <Drawer open={openDrawer} onClose={onDrawerClose}/>
    </>
  );
}

export default connectService(RoomsContext, (
  {rooms}, roomsService) => ({rooms, roomsService})
)(
  connectService(ContactsContext, (
    {names}, contactsService) => ({names, contactsService})
  )(
    connectService(MyProfileContext, ({profile, profileReady},) => ({
        profile, profileReady
      })
    )(withStyles(headerStyles)(Header)
    )
  )
);
