import React, {useState} from 'react';
import {useHistory} from 'react-router-dom';
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

function HeaderView({classes, menuAnchor, onOpenMenu, onCloseMenu, onLogout, openDrawer, onDownloadKey}) {
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
        <IconButton onClick={e => onOpenMenu(e)} color="inherit">
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
          <MenuItem onClick={onLogout}>
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

function Header({classes, onLogout, openDrawer, privateKey}) {
  const [menuAnchor, setMenuAnchor] = useState(false);
  const history = useHistory();

  const props = {
    classes,
    menuAnchor,
    openDrawer,

    onOpenMenu(event) {
      setMenuAnchor(event.currentTarget);
    },

    onCloseMenu() {
      setMenuAnchor(null);
    },

    onDownloadKey() {
      FileSaver.saveAs(new File([privateKey], 'key.dat', {
        type: 'text/plain;charset=utf-8'
      }));
      props.onCloseMenu();
    },

    onLogout() {
      onLogout();
      props.onCloseMenu();
      history.push('/');
    }
  };

  return <HeaderView {...props}/>
}

export default withStyles(headerStyles)(
  connectService(AuthContext, ({privateKey}, authService) => ({
      privateKey,
      onLogout() {
        authService.logout();
      }
    })
  )(Header)
);
