import React, {useEffect, useState} from 'react';
import {useRouteMatch} from 'react-router-dom';
import {getInstance, useService} from 'global-apps-common';
import DeleteMenu from '../DeleteMenu/DeleteMenu';
import {withStyles} from "@material-ui/core";
import CircularProgress from '../CircularProgress/CircularProgress';
import Typography from '@material-ui/core/Typography';
import Divider from '@material-ui/core/Divider';
import Breadcrumbs from '../Breadcrumbs/Breadcrumbs';
import itemListStyles from './ItemListStyles';
import FolderIcon from '@material-ui/icons/FolderOpenOutlined';
import FileViewer from '../FileViewer/FileViewer';
import {getItemContainer} from "./getItemContainer";
import {withSnackbar} from "notistack";
import FSService from "../../modules/fs/FSService";

function ItemListView({
                        classes,
                        fileList,
                        loading,
                        path,
                        contextStyles,
                        isFileViewerOpen,
                        selectedItem,
                        openFileViewer,
                        onOpenDeleteMenu,
                        onFileViewerClose,
                        onDeleteItem
                      }) {
  return (
    <div className={classes.root}>
      <Breadcrumbs/>
      <Divider/>
      {loading && (
        <div className={classes.wrapper}>
          <CircularProgress/>
        </div>
      )}
      {fileList.length > 0 && !loading &&
      getItemContainer(
        path,
        fileList,
        openFileViewer,
        onOpenDeleteMenu,
        classes
      )
      }
      {fileList.length <= 0 && !loading && (
        <div className={classes.wrapper}>
          <FolderIcon className={classes.emptyIndicator}/>
          <Typography variant="h5">
            Directory is empty
          </Typography>
        </div>
      )}
      {selectedItem && isFileViewerOpen && (
        <FileViewer
          file={selectedItem}
          onClose={onFileViewerClose}
          onDelete={onDeleteItem}
        />
      )}
      <DeleteMenu
        deleteHandler={onDeleteItem}
        style={contextStyles}
      />
    </div>
  );
}

function ItemList({classes, enqueueSnackbar}) {
  const fsService = getInstance(FSService);
  const {directories, files, path, loading} = useService(fsService);
  const [contextStyles, setContextStyles] = useState({});
  const [isFileViewerOpen, setIsFileViewerOpen] = useState(false);
  const [selectedItem, setSelectedItem] = useState(null);
  const match = useRouteMatch();

  useEffect(() => {
    fsService.fetch(getCurrentPath())
      .then(() => window.addEventListener('click', removeContextMenu))
      .catch(error => enqueueSnackbar(error.message, {
        variant: 'error'
      }));
    return window.removeEventListener('click', removeContextMenu)
  }, [match]);

  function getCurrentPath() {
    return match.params[0] || '/';
  }

  function removeContextMenu() {
    setContextStyles({open: false});
  }

  const props = {
    classes,
    fileList: [...directories, ...files],
    loading,
    path,
    match,
    contextStyles,
    isFileViewerOpen,
    selectedItem,

    onOpenDeleteMenu(name, isDirectory, e) {
      e.preventDefault();
      const {clientX, clientY} = e;
      setContextStyles({
        open: true,
        top: clientY,
        left: clientX
      });
      setSelectedItem({
        name: name,
        isDirectory
      });
    },

    onDeleteItem() {
      if (selectedItem.isDirectory) {
        fsService.rmdir(selectedItem.name)
          .catch(error => enqueueSnackbar(error.message, {
            variant: 'error'
          }));
      } else {
        fsService.rmfile(selectedItem.name)
          .catch(error => enqueueSnackbar(error.message, {
            variant: 'error'
          }));
        setIsFileViewerOpen(false);
        setSelectedItem(null);
      }
    },

    openFileViewer(item) {
      setSelectedItem(item);
      setIsFileViewerOpen(true);
    },

    onFileViewerClose() {
      setSelectedItem(null);
      setIsFileViewerOpen(false);
    }
  };

  return <ItemListView {...props}/>
}

export default withSnackbar(
  withStyles(itemListStyles)(ItemList)
);
