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

function Header({classes, rooms, roomId}) {
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
            aria-label="Open drawer"
            onClick={onDrawerOpen}
            edge="start"
            className={classes.iconButton}
          >
            <MenuIcon className={classes.menuIcon}/>
          </ListItemIcon>
          <Typography
            color="inherit"
            variant="h6"
            className={classes.title}
          >
            Global Chat
          </Typography>
          <div className={classes.chatTitleContainer}>
            {rooms.get(roomId) !== undefined && (
              <Typography
                className={classes.chatTitle}
                color="inherit"
              >
                <ListItemIcon className={classes.listItemIcon}>
                  <ArrowIcon className={classes.arrowIcon}/>
                </ListItemIcon>
                {rooms.get(roomId).name}
              </Typography>
            )}
          </div>
        </Toolbar>
      </AppBar>
      <Drawer
        open={openDrawer}
        onClose={onDrawerClose}
      />
    </>
  );
}

export default connectService(RoomsContext, (
  {ready, rooms}, roomsService) => ({ready, rooms, roomsService})
)(
  withStyles(headerStyles)(Header)
);
