import React from 'react';
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
import AuthContext from '../../modules/auth/AuthContext';
import connectService from '../../common/connectService';
import headerStyles from "./headerStyles";

class Header extends React.Component {
  state = {
    menuAnchor: false
  };

  openMenu = (event) => {
    this.setState({
      menuAnchor: event.currentTarget
    })
  };

  downloadKey = () => {
    FileSaver.saveAs(this.props.authService.createKeysFile());
    this.closeMenu();
  };

  logout = () => {
    this.props.authService.logout();
    this.closeMenu();
    this.props.history.push('/');
  };

  closeMenu = () => {
    this.setState({
      menuAnchor: null
    })
  };

  render() {
    return (
      <AppBar
        position="static"
        color="inherit"
        classes={{
          root: this.props.classes.root
        }}
      >
        <Toolbar>
          <IconButton onClick={this.props.openDrawer} color="inherit" className={this.props.classes.drawerTrigger}>
            <MenuIcon/>
          </IconButton>
          <Typography variant="h5" color="inherit" className={this.props.classes.logo}>
            Global Files
          </Typography>
          <div className={this.props.classes.grow}/>
          <IconButton onClick={this.openMenu} color="inherit" aria-label="Menu">
            <AccountCircleIcon/>
          </IconButton>
        </Toolbar>
        <Menu
          disableAutoFocusItem
          anchorEl={this.state.menuAnchor}
          open={Boolean(this.state.menuAnchor)}
          onClose={this.closeMenu}
        >
          <MenuList>
            <MenuItem onClick={this.downloadKey}>
              <ListItemIcon>
                <DownloadIcon/>
              </ListItemIcon>
              <ListItemText inset primary="Download key"/>
            </MenuItem>
            <MenuItem onClick={this.logout}>
              <ListItemIcon>
                <ExitIcon/>
              </ListItemIcon>
              <ListItemText inset primary="Logout"/>
            </MenuItem>
          </MenuList>
        </Menu>
      </AppBar>
    );
  }
}

export default withRouter(
  withStyles(headerStyles)(
    connectService(AuthContext, (store, authService) => ({authService}))(Header)
  )
);
