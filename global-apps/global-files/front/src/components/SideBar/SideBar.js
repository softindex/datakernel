import React, {useRef, useState} from 'react';
import {getInstance, useSnackbar} from 'global-apps-common';
import PromptDialog from '../PromptDialog/PromptDialog';
import {withStyles} from "@material-ui/core";
import Drawer from '@material-ui/core/Drawer';
import AddIcon from '@material-ui/icons/AddCircleOutline';
import Fab from '@material-ui/core/Fab';
import withWidth, {isWidthDown} from '@material-ui/core/withWidth';
import sideBarStyles from './sideBarStyles';
import FSService from "../../modules/fs/FSService";
import CreateFolderMenu from "../CreateFolderMenu/CreateFolderMenu";
import SideBarMenu from "../SideBarMenu/SideBarMenu";

function SideBarView({
                       classes,
                       width,
                       folderFormIsOpen,
                       onSubmit,
                       onDialogClose,
                       isDrawerOpen,
                       onDrawerClose,
                       onOpenFabMenu,
                       fabElement,
                       onCloseFabMenu,
                       onDialogOpen,
                       onFileUpload,
                       inputRef
                     }) {
  return (
    <Drawer
      classes={{paper: classes.root}}
      variant={isWidthDown('md', width) ?
        'persistant' : 'permanent'
      }
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
      <CreateFolderMenu
        fabElement={fabElement}
        onCloseFabMenu={onCloseFabMenu}
        onDialogOpen={onDialogOpen}
        onFileUpload={onFileUpload}
        inputRef={inputRef}
      />
      <SideBarMenu onDrawerClose={onDrawerClose}/>
      {folderFormIsOpen && (
        <PromptDialog
          title="Create folder"
          onClose={onDialogClose}
          onSubmit={onSubmit}
        />
      )}
    </Drawer>
  );
}

function SideBar({classes, width, isDrawerOpen, onDrawerClose}) {
  const fsService = getInstance(FSService);
  const [fabElement, setFabElement] = useState(null);
  const [folderFormIsOpen, setFolderFormIsOpen] = useState(false);
  const {showSnackbar} = useSnackbar();
  const inputRef = useRef();

  const props = {
    classes,
    width,
    fabElement,
    folderFormIsOpen,
    isDrawerOpen,
    onDrawerClose,
    inputRef,

    onFileUpload() {
      props.onCloseFabMenu();
      onDrawerClose();
      Promise.all([...inputRef.current.files].map(file => {
        fsService.writeFile(file)
          .catch(err => showSnackbar(err.message, 'error'));
      }));
    },

    onCloseFabMenu() {
      setFabElement(null);
    },

    onDialogOpen() {
      props.onCloseFabMenu();
      setFolderFormIsOpen(true);
    },

    onDialogClose() {
      setFolderFormIsOpen(false);
    },

    onSubmit(name) {
      if (name) {
        setFolderFormIsOpen(false);
        onDrawerClose();
        if (!name) {
          return;
        }
        fsService.mkdir(name)
          .catch(err => showSnackbar(err.message, 'error'));
      }
    },

    onOpenFabMenu(event) {
      setFabElement(event.currentTarget);
    },
  };

  return <SideBarView {...props}/>
}


export default withWidth()(
  withStyles(sideBarStyles)(SideBar)
);
