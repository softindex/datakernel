import React, {useEffect} from 'react';
import {getInstance, useService} from 'global-apps-common';
import {withStyles} from "@material-ui/core";
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import CircularProgress from '../CircularProgress';
import uploadingStyles from '../Uploading/uploadingStyles';
import {getFileTypeByName} from '../../common/utils';
import FileTypeIcon from '../FileTypeIcon';
import UploadingAlert from "../UploadingAlert";
import FSService from "../../modules/fs/FSService";

function UploadingView({files, uploads, classes, onClose}) {
  const items = uploads.map(item => {
    const fileIsFinallyAdd = Boolean(files.find(({name}) => name === item.name));
    return (
      <ListItem key={item.name} className={classes.item}>
        <FileTypeIcon
          type={getFileTypeByName(item.name)}
          className={classes.fileIcon}
        />
        <ListItemText
          primaryTypographyProps={{
            noWrap: true,
            variant: 'body2'
          }}
          primary={item.name}
        />
        <CircularProgress
          value={item.upload}
          isError={Boolean(item.error)}
          success={fileIsFinallyAdd}
          size={24}
        />
      </ListItem>
    );
  });

  if (uploads.length) {
    return (
      <UploadingAlert
        items={items}
        onClose={onClose}
        uploads={uploads}
      />
    )
  }

  return null;
}

function Uploading({classes}) {
  const fsService = getInstance(FSService);
  const {files, uploads} = useService(fsService);

  useEffect(() => {
    if (uploads.length !== 0) {
      setTimeout(() => fsService.clearUploads(), 4000);
    }
  }, [uploads]);

  const props = {
    classes,
    uploads: [...uploads.values()],
    files,
    onClose: () => fsService.clearUploads()
  };

  return <UploadingView {...props}/>
}

export default withStyles(uploadingStyles)(Uploading);
