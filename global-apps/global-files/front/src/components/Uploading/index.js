import React from 'react';
import connectService from '../../common/connectService';
import FSContext from '../../modules/fs/FSContext';
import {withStyles} from "@material-ui/core";
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import CircularProgress from '../CircularProgress';
import uploadingStyles from '../Uploading/uploadingStyles';
import {getFileTypeByName} from '../../common/utils';
import FileTypeIcon from '../FileTypeIcon';
import UploadingAlert from "../UploadingAlert";

/**
 * @return {null}
 */
function Uploading({files, uploads, classes, onClose}) {
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

export default withStyles(uploadingStyles)(
  connectService(FSContext, ({uploads, files}, fsService) => ({
      uploads: [...uploads.values()],
      files,
      onClose: () => fsService.clearUploads()
    })
  )(
    Uploading
  )
);
