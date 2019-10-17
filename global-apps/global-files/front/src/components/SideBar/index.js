import React from 'react';
import FSContext from '../../modules/fs/FSContext';
import connectService from '../../common/connectService';
import PromptDialog from '../PromptDialog/PromptDialog';
import {withStyles} from "@material-ui/core";
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import CreateFolderIcon from '@material-ui/icons/CreateNewFolderOutlined';
import StorageIcon from '@material-ui/icons/StorageOutlined';
import AddIcon from '@material-ui/icons/AddCircleOutline';
import FileIcon from '@material-ui/icons/InsertDriveFileOutlined';
import Fab from '@material-ui/core/Fab';
import Menu from '@material-ui/core/Menu';
import MenuList from '@material-ui/core/MenuList';
import MenuItem from '@material-ui/core/MenuItem';
import Divider from '@material-ui/core/Divider';
import Snackbar from '../Snackbar/Snackbar';
import withWidth, {isWidthDown} from '@material-ui/core/withWidth';
import Link from "react-router-dom/es/Link";
import sideBarStyles from './sideBarStyles';

class Index extends React.Component {
  state = {
    fabElement: null,
    folderFormIsOpen: false,
    isOpen: false,
    error: null
  };

  onFileUpload = () => {
    this.closeFabMenu();
    this.props.onDrawerClose();


    Promise.all([...this.input.files].map(async file => {
      try {
        await this.props.fsService.writeFile(file)
      } catch (e) {
        console.error(e);
      }
    }));
  };

  createFolder = async (name) => {
    this.props.onDrawerClose();

    if (!name) {
      return;
    }

    try {
      await this.props.fsService.mkdir(name);
    } catch (e) {
      this.setState({
        error: e.message
      })
    }
  };

  openFabMenu = (event) => {
    this.setState({
      fabElement: event.currentTarget
    })
  };

  closeFabMenu = () => {
    this.setState({
      fabElement: null
    })
  };

  formOpen = () => {
    this.closeFabMenu();
    this.setState({
      folderFormIsOpen: true
    });
  };

  formClose = () => {
    this.setState({
      folderFormIsOpen: false
    })
  };

  formSubmit = (name) => {
    if (name) {
      this.setState({
        folderFormIsOpen: false
      });
      this.createFolder(name);
    }
  };

  render() {
    return (
      <Drawer
        classes={{
          paper: this.props.classes.root
        }}
        variant={isWidthDown('md', this.props.width) ? 'persistant' : 'permanent'}
        open={this.props.isDrawerOpen}
        onClose={this.props.onDrawerClose}
      >
        <Fab
          variant="extended"
          aria-label="Add"
          className={this.props.classes.fab}
          onClick={this.openFabMenu}
        >
          <AddIcon className={this.props.classes.icon}/>
          Create
        </Fab>
        <Menu
          open={Boolean(this.state.fabElement)}
          onClose={this.closeFabMenu}
          disableAutoFocusItem
          anchorEl={this.state.fabElement}
        >
          <MenuList>
            <MenuItem onClick={this.formOpen}>
              <ListItemIcon>
                <CreateFolderIcon/>
              </ListItemIcon>
              <ListItemText inset primary="Create folder"/>
            </MenuItem>
            <Divider/>
            <input
              onChange={this.onFileUpload}
              ref={ref => this.input = ref}
              multiple type="file"
              id="file"
              className={this.props.classes.uploadInput}
            />
            <label htmlFor="file">
              <MenuItem>
                <ListItemIcon>
                  <FileIcon/>
                </ListItemIcon>
                <ListItemText inset primary="Upload File"/>
              </MenuItem>
            </label>
          </MenuList>
        </Menu>
        <List className={this.props.classes.menuList}>
          <Link to="/folders" style={{textDecoration: 'none'}}>
            <ListItem
              button
              selected={true}
              onClick={this.props.onDrawerClose}
              classes={{
                root: this.props.classes.listItem,
                selected: this.props.classes.listItemSelected
              }}
            >
              <ListItemIcon>
                <StorageIcon/>
              </ListItemIcon>
              <ListItemText
                primaryTypographyProps={{
                  classes: {
                    root: this.props.classes.listTypography
                  }
                }}
                primary="My files"
              />
            </ListItem>
          </Link>
        </List>
        <PromptDialog
          title="Create folder"
          open={this.state.folderFormIsOpen}
          onClose={this.formClose}
          onSubmit={this.formSubmit}
        />
        <Snackbar
          error={this.state.error}
        />
      </Drawer>
    );
  }
}

export default withWidth()(withStyles(sideBarStyles)(
  connectService(FSContext, (state, fsService) => ({fsService}))(Index)
));
