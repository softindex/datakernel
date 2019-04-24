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

class FileViewer extends React.Component {
  render() {
    return (
      <Dialog
        fullScreen
        open={this.props.open}
        onClose={this.props.onClose}
        TransitionComponent={Fade}
        className={this.props.classes.root}
      >
        <AppBar
          className={this.props.classes.appBar} color="default"
          position="static"
        >
          <Toolbar>
            <IconButton
              color="inherit"
              onClick={this.props.onClose}
              aria-label="Close"
              className={this.props.classes.closeIcon}
            >
              <CloseIcon color="inherit"/>
            </IconButton>
            <Typography className={this.props.classes.title} variant="subtitle1" color="inherit">
              {this.props.file.name}
            </Typography>
            <div className={this.props.classes.grow}/>
            <IconButton
              className={this.props.classes.icon}
              aria-label="Delete"
              onClick={this.props.onDelete}
            >
              <DeleteIcon/>
            </IconButton>
            <IconButton
              className={this.props.classes.icon}
              download
              href={this.props.file.downloadLink}
              aria-label="Download"
            >
              <DownloadIcon/>
            </IconButton>
          </Toolbar>
        </AppBar>

        <div className={this.props.classes.content}>
          {(getFileTypeByName(this.props.file.name) === 'image') && (
            <img
              className={this.props.classes.image}
              alt={this.props.file.name}
              src={this.props.file.downloadLink}
            />
          )}
          {(getFileTypeByName(this.props.file.name) !== 'image') && (
            <Paper elevation={10} className={this.props.classes.noPreviewContainer}>
              <Typography gutterBottom variant="subtitle1"> No preview available </Typography>
              <Button
                download
                href={this.props.file.downloadLink}
                variant="outlined"
              >
                <DownloadIcon className={this.props.classes.downloadIcon}/>
                Download
              </Button>
            </Paper>
          )}
        </div>

      </Dialog>
    )
  }
}

export default withStyles(fileViewerStyles)(FileViewer);
