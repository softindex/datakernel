import React, {useRef, useState} from 'react';
import FSContext from '../../modules/fs/FSContext';
import connectService from '../../common/connectService';
import PromptDialog from '../PromptDialog';
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
import Snackbar from '../Snackbar';
import withWidth, {isWidthDown} from '@material-ui/core/withWidth';
import Link from "react-router-dom/es/Link";
import sideBarStyles from './sideBarStyles';
import {withSnackbar} from "notistack";

function SideBar({classes, fsService, width, isDrawerOpen, onDrawerClose, enqueueSnackbar}) {
  const [fabElement, setFabElement] = useState(null);
  const [folderFormIsOpen, setFolderFormIsOpen] = useState(false);
  const [error, setError] = useState(null);
  const inputRef = useRef();

  const onFileUpload = () => {
    onCloseFabMenu();
    onDrawerClose();
    Promise.all([...inputRef.current.files].map(async file => {
      try {
        await fsService.writeFile(file)
      } catch (err) {
        enqueueSnackbar(err.message, {
          variant: 'error'
        });
      }
    }));
  };

  const onCreateFolder = async (name) => {
    onDrawerClose();

    if (!name) {
      return;
    }

    try {
      await fsService.mkdir(name);
    } catch (e) {
      setError(e.message);
    }
  };

  const onOpenFabMenu = (event) => {
    setFabElement(event.currentTarget);
  };

  const onCloseFabMenu = () => {
    setFabElement(null);
  };

  const onDialogOpen = () => {
    onCloseFabMenu();
    setFolderFormIsOpen(true);
  };

  const onDialogClose = () => {
    setFolderFormIsOpen(false);
  };

  const onSubmit = (name) => {
    if (name) {
      setFolderFormIsOpen(false);
      onCreateFolder(name)
        .catch((err) => {
          enqueueSnackbar(err.message, {
            variant: 'error'
          });
        });
    }
  };

  return (
    <Drawer
      classes={{
        paper: classes.root
      }}
      variant={isWidthDown('md', width) ? 'persistant' : 'permanent'}
      open={isDrawerOpen}
      onClose={onDrawerClose}
    >
      <Fab
        variant="extended"
        className={classes.fab}
        onClick={onOpenFabMenu}
      >
        <AddIcon className={classes.icon}/>
        Create
      </Fab>
      <Menu
        open={Boolean(fabElement)}
        onClose={onCloseFabMenu}
        disableAutoFocusItem
        anchorEl={fabElement}
      >
        <MenuList>
          <MenuItem onClick={onDialogOpen}>
            <ListItemIcon className={classes.listItemIcon}>
              <CreateFolderIcon/>
            </ListItemIcon>
            <ListItemText
              className={classes.litItemText}
              primary="Create folder"
            />
          </MenuItem>
          <Divider/>
          <input
            onChange={onFileUpload}
            ref={inputRef}
            multiple type="file"
            id="file"
            className={classes.uploadInput}
          />
          <label htmlFor="file">
            <MenuItem>
              <ListItemIcon className={classes.listItemIcon}>
                <FileIcon/>
              </ListItemIcon>
              <ListItemText
                className={classes.litItemText}
                primary="Upload File"
              />
            </MenuItem>
          </label>
        </MenuList>
      </Menu>
      <List className={classes.menuList}>
        <Link to="/folders" className={classes.foldersLink}>
          <ListItem
            button
            selected={true}
            onClick={onDrawerClose}
            classes={{
              root: classes.listItem,
              selected: classes.listItemSelected
            }}
          >
            <ListItemIcon>
              <StorageIcon/>
            </ListItemIcon>
            <ListItemText
              primaryTypographyProps={{
                classes: {
                  root: classes.listTypography
                }
              }}
              primary="My files"
            />
          </ListItem>
        </Link>
      </List>
      <PromptDialog
        title="Create folder"
        open={folderFormIsOpen}
        onClose={onDialogClose}
        onSubmit={onSubmit}
      />
      <Snackbar error={error}/>
    </Drawer>
  );
}

export default withWidth()(
  withSnackbar(
    withStyles(sideBarStyles)(
      connectService(FSContext, (state, fsService) => ({fsService}))(SideBar)
    )
  )
);
