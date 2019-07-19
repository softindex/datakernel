import React from 'react';
import connectService from '../../common/connectService';
import FSContext from '../../modules/fs/FSContext';
import {withStyles} from "@material-ui/core";
import Typography from '@material-ui/core/Typography';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemText from '@material-ui/core/ListItemText';
import CircularProgress from '../theme/CircularProgress';
import CloseIcon from '@material-ui/icons/Close';
import IconButton from '@material-ui/core/IconButton';
import uploadingStyles from '../Uploading/uploadingStyles';
import Paper from '@material-ui/core/Paper';
import {getFileTypeByName} from '../../common/utils';
import FileTypeIcon from '../theme/FileTypeIcon';

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
      <Paper className={classes.root}>
        <div className={classes.header}>
          <Typography color="inherit" variant="subtitle1" className={classes.title}>
            {uploads.length} uploads complete
          </Typography>
          <IconButton color="inherit" onClick={onClose}>
            <CloseIcon/>
          </IconButton>
        </div>
        <div className={classes.body}>
          <List> {items} </List>
        </div>
      </Paper>
    );
  }

  return null;
}
  
export default withStyles(uploadingStyles)(connectService(FSContext, ({uploads, files}, fsService) => ({
  uploads: [...uploads.values()],
  files,
  onClose: () => fsService.clearUploads()
}))(Uploading));
