import React, {useRef, useState} from 'react';
import {getInstance} from 'global-apps-common';
import PromptDialog from '../PromptDialog/PromptDialog';
import {withStyles} from "@material-ui/core";
import Drawer from '@material-ui/core/Drawer';
import AddIcon from '@material-ui/icons/AddCircleOutline';
import Fab from '@material-ui/core/Fab';
import Snackbar from '../Snackbar/Snackbar';
import withWidth, {isWidthDown} from '@material-ui/core/withWidth';
import sideBarStyles from './sideBarStyles';
import {withSnackbar} from "notistack";
import FSService from "../../modules/fs/FSService";
import CreateFolderMenu from "../CreateFolderMenu/CreateFolderMenu";
import SideBarMenu from "../SideBarMenu/SideBarMenu";

function SideBarView({
                       classes,
                       width,
                       folderFormIsOpen,
                       onSubmit,
                       error,
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
      classes={{
        paper: classes.root
      }}
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
      <Snackbar error={error}/>
    </Drawer>
  );
}

function SideBar({classes, width, enqueueSnackbar, isDrawerOpen, onDrawerClose}) {
  const fsService = getInstance(FSService);

  const [fabElement, setFabElement] = useState(null);
  const [folderFormIsOpen, setFolderFormIsOpen] = useState(false);
  const [error, setError] = useState(null);
  const inputRef = useRef();

  const props = {
    classes,
    width,
    fabElement,
    folderFormIsOpen,
    isDrawerOpen,
    onDrawerClose,
    error,
    inputRef,

    onFileUpload() {
      props.onCloseFabMenu();
      onDrawerClose();
      Promise.all([...inputRef.current.files].map(file => {
        fsService.writeFile(file)
          .catch((err) => {
            enqueueSnackbar(err.message, {
              variant: 'error'
            });
          });
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
          .catch((err) => {
            setError(err.message);
          });
      }
    },

    onOpenFabMenu(event) {
      setFabElement(event.currentTarget);
    },
  };

  return <SideBarView {...props}/>
}


export default withWidth()(
  withSnackbar(
    withStyles(sideBarStyles)(SideBar)
  )
);
