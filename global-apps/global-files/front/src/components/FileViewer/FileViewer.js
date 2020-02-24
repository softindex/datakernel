import React from 'react';
import fileViewerStyles from './fileViewerStyles';
import {withStyles} from "@material-ui/core";
import Dialog from '@material-ui/core/Dialog';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import IconButton from '@material-ui/core/IconButton';
import Typography from '@material-ui/core/Typography';
import CloseIcon from '@material-ui/icons/Close';
import DownloadIcon from '@material-ui/icons/CloudDownloadOutlined';
import DeleteIcon from '@material-ui/icons/DeleteForeverOutlined';
import Fade from '@material-ui/core/Fade';
import {getFileTypeByName} from '../../common/utils';
import Paper from '@material-ui/core/Paper';
import Button from '@material-ui/core/Button';

function FileViewer({classes, onClose, file, onDelete}) {
  return (
    <Dialog
      fullScreen
      open={true}
      onClose={onClose}
      TransitionComponent={Fade}
      className={classes.root}
    >
      <AppBar
        className={classes.appBar}
        color="default"
        position="static"
      >
        <Toolbar>
          <IconButton
            color="inherit"
            onClick={onClose}
            className={classes.closeIcon}
          >
            <CloseIcon color="inherit"/>
          </IconButton>
          <Typography
            className={classes.title}
            variant="subtitle1"
            color="inherit"
          >
            {file.name}
          </Typography>
          <div className={classes.grow}/>
          <IconButton
            className={classes.icon}
            onClick={onDelete}
          >
            <DeleteIcon/>
          </IconButton>
          <IconButton
            className={classes.icon}
            download
            href={file.downloadLink}
          >
            <DownloadIcon/>
          </IconButton>
        </Toolbar>
      </AppBar>
      <div className={classes.content}>
        {(getFileTypeByName(file.name) === 'image') && (
          <img
            className={classes.image}
            alt={file.name}
            src={file.downloadLink}
          />
        )}
        {(getFileTypeByName(file.name) !== 'image') && (
          <Paper elevation={10} className={classes.noPreviewContainer}>
            <Typography
              gutterBottom
              variant="subtitle1"
            >
              No preview available
            </Typography>
            <Button
              download
              href={file.downloadLink}
              variant="outlined"
            >
              <DownloadIcon className={classes.downloadIcon}/>
              Download
            </Button>
          </Paper>
        )}
      </div>
    </Dialog>
  )
}

export default withStyles(fileViewerStyles)(FileViewer);
