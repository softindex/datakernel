import React from 'react';
import {withRouter} from 'react-router-dom';
import FSContext from '../../modules/fs/FSContext';
import {connectService} from 'global-apps-common';
import DeleteMenu from '../DeleteMenu';
import {withStyles} from "@material-ui/core";
import CircularProgress from '@material-ui/core/CircularProgress';
import Typography from '@material-ui/core/Typography';
import Divider from '@material-ui/core/Divider';
import Breadcrumbs from '../Breadcrumbs';
import itemListStyles from './ItemListStyles';
import FolderIcon from '@material-ui/icons/FolderOpenOutlined';
import FileViewer from '../FileViewer';
import {getItemContainer} from "./getItemContainer";
import {withSnackbar} from "notistack";

class ItemList extends React.Component {
  state = {
    contextStyles: {},
    isFileViewerOpen: false,
    selectedItem: null
  };

  async componentDidMount() {
    await this.props.fsService.fetch(this.getCurrentPath());
    window.addEventListener('click', this.removeContextMenu);
  }

  componentWillUnmount() {
    window.removeEventListener('click', this.removeContextMenu);
  }

  componentDidUpdate(prevProps) {
    if (this.props.match !== prevProps.match) {
      this.props.fsService.fetch(this.getCurrentPath());
    }
  }

  removeContextMenu = () => {
    this.setState({contextStyles: {open: false}});
  };

  handleContextMenu = (name, isDirectory, e) => {
    e.preventDefault();
    const {clientX, clientY} = e;
    this.setState({
      contextStyles: {
        open: true,
        top: clientY,
        left: clientX
      },
      selectedItem: {
        name: name,
        isDirectory
      }
    });
  };

  onDeleteItem = async () => {
    try {
      if (this.state.selectedItem.isDirectory) {
        await this.props.fsService.rmdir(this.state.selectedItem.name);
      } else {
        await this.props.fsService.rmfile(this.state.selectedItem.name);
        this.setState({
          isFileViewerOpen: false,
          selectedItem: null
        });
      }
    } catch (err) {
      this.props.enqueueSnackbar(err.message, {
        variant: 'error'
      });
    }
  };

  getCurrentPath() {
    return this.props.match.params[0] || '/';
  }

  openFileViewer = (item) => {
    this.setState({
      selectedItem: item,
      isFileViewerOpen: true
    })
  };

  onFileViewerClose = () => {
    this.setState({
      selectedItem: null,
      isFileViewerOpen: false
    })
  };

  render() {
    return (
      <div className={this.props.classes.root}>
        <Breadcrumbs fsService={this.props.fsService}/>
        <Divider/>
        {this.props.loading && (
          <div className={this.props.classes.wrapper}>
            <CircularProgress/>
          </div>
        )}
        {this.props.fileList.length > 0 && !this.props.loading &&
        getItemContainer(
          this.props.path,
          this.props.fileList,
          this.openFileViewer,
          this.handleContextMenu,
          this.props.classes
        )
        }
        {this.props.fileList.length <= 0 && !this.props.loading && (
          <div className={this.props.classes.wrapper}>
            <FolderIcon className={this.props.classes.emptyIndicator}/>
            <Typography variant="h5">
              Directory is empty
            </Typography>
          </div>
        )}
        {this.state.selectedItem && this.state.isFileViewerOpen && (
          <FileViewer
            file={this.state.selectedItem}
            onClose={this.onFileViewerClose}
            onDelete={this.onDeleteItem}
          />
        )}
        <DeleteMenu
          deleteHandler={this.onDeleteItem}
          style={this.state.contextStyles}
        />
      </div>
    );
  }
}

export default withSnackbar(
  withStyles(itemListStyles)(
    connectService(FSContext, (
      {directories, files, path, loading}, fsService) => ({
        fileList: [...directories, ...files],
        path,
        loading,
        fsService
      })
    )(
      withRouter(ItemList)
    )
  )
);
