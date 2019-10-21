import React, {useState} from 'react';
import {withRouter} from 'react-router-dom';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import IconButton from '@material-ui/core/IconButton';
import AccountCircleIcon from '@material-ui/icons/AccountCircleOutlined';
import MenuIcon from '@material-ui/icons/MenuOutlined';
import DownloadIcon from '@material-ui/icons/CloudDownloadOutlined';
import ExitIcon from '@material-ui/icons/ExitToAppOutlined';
import {withStyles} from "@material-ui/core";
import Menu from '@material-ui/core/Menu';
import MenuList from '@material-ui/core/MenuList';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import MenuItem from '@material-ui/core/MenuItem';
import FileSaver from 'file-saver';
import {connectService, AuthContext} from 'global-apps-common';
import headerStyles from "./headerStyles";

function Header({classes, history, onLogout, openDrawer, onCreateKeysFile}) {
  const [menuAnchor, setMenuAnchor] = useState(false);

  const onOpenMenu = event => {
    setMenuAnchor(event.currentTarget);
  };

  const onCloseMenu = () => {
    setMenuAnchor(null);
  };

  const onDownloadKey = () => {
    FileSaver.saveAs(onCreateKeysFile());
    onCloseMenu();
  };

  const onClickLogout = () => {
    onLogout();
    onCloseMenu();
    history.push('/');
  };

  return (
    <AppBar
      position="static"
      color="inherit"
      classes={{
        root: classes.root
      }}
    >
      <Toolbar>
        <IconButton
          onClick={openDrawer}
          color="inherit"
          className={classes.drawerTrigger}
        >
          <MenuIcon/>
        </IconButton>
        <Typography
          variant="h5"
          color="inherit"
          className={classes.logo}
        >
          Global Files
        </Typography>
        <div className={classes.grow}/>
        <IconButton onClick={onOpenMenu} color="inherit">
          <AccountCircleIcon/>
        </IconButton>
      </Toolbar>
      <Menu
        disableAutoFocusItem
        anchorEl={menuAnchor}
        open={Boolean(menuAnchor)}
        onClose={onCloseMenu}
      >
        <MenuList>
          <MenuItem onClick={onDownloadKey}>
            <ListItemIcon className={classes.listItemIcon}>
              <DownloadIcon/>
            </ListItemIcon>
            <ListItemText
              className={classes.listItemText}
              primary="Download key"
            />
          </MenuItem>
          <MenuItem onClick={onClickLogout}>
            <ListItemIcon className={classes.listItemIcon}>
              <ExitIcon/>
            </ListItemIcon>
            <ListItemText
              className={classes.listItemText}
              primary="Logout"
            />
          </MenuItem>
        </MenuList>
      </Menu>
    </AppBar>
  );
}

export default withRouter(
  withStyles(headerStyles)(
    connectService(AuthContext, (store, authService) => ({
        authService,
        onLogout() {
          authService.logout();
        },
        onCreateKeysFile() {
          return authService.createKeysFile();
        }
      })
    )(
      Header
    )
  )
);
