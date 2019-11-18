import React, {useState} from 'react';
import {ListItemIcon, withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import ArrowIcon from '@material-ui/icons/KeyboardArrowRight'
import headerStyles from './headerStyles';
import MenuIcon from "@material-ui/icons/Menu";
import Drawer from "../Drawer/Drawer";

function Header({classes, title}) {
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
            {title !== '' && (
              <>
                <ListItemIcon className={classes.listItemIcon}>
                  <ArrowIcon className={classes.arrowIcon}/>
                </ListItemIcon>
                <Typography
                  className={classes.chatTitle}
                  color="inherit"
                >
                  {title}
                </Typography>
              </>
            )}
          </div>
        </Toolbar>
      </AppBar>
      <Drawer open={openDrawer} onClose={onDrawerClose}/>
    </>
  );
}

export default withStyles(headerStyles)(Header);
