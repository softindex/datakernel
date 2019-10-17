import React from 'react';
import {withRouter} from 'react-router-dom';
import FSContext from '../../modules/fs/FSContext';
import connectService from '../../common/connectService';
import ContextMenu from '../ContextMenu/ContextMenu';
import ItemCard from '../ItemCard/ItemCard';
import {withStyles} from "@material-ui/core";
import CircularProgress from '@material-ui/core/CircularProgress';
import Typography from '@material-ui/core/Typography';
import Divider from '@material-ui/core/Divider';
import Breadcrumbs from '../Breadcrumbs';
import itemListStyles from './ItemListStyles';
import FolderIcon from '@material-ui/icons/FolderOpenOutlined';
import FileViewer from '../FileViewer';

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
    } catch (e) {
      alert(e.message);
    }
  };

  getCurrentPath() {
    return this.props.match.params[0] || '/';
  }

  getItemsByType = () => {
    let folders = [];
    let files = [];

    for (const item of this.props.fileList) {

      if (item.isDirectory) {
        folders.push(
            <ItemCard
              name={item.name}
              isDirectory={true}
              path={this.props.path}
              onContextMenu={this.handleContextMenu.bind(null, item.name, item.isDirectory)}
            />
        )
      } else {
        files.push(
          <ItemCard
            name={item.name}
            isDirectory={false}
            onClick={this.openFileViewer.bind(null, item)}
            onContextMenu={this.handleContextMenu.bind(null, item.name, item.isDirectory)}
          />
        )
      }
    }

    return {
      folders,
      files
    }

  };

  getItemContainer = () => {
    const items = this.getItemsByType();

    return (
      <React.Fragment>
        {items.folders.length > 0 && (
          <div className={this.props.classes.section}>
            <Typography variant="h6" gutterBottom>
              Folders
            </Typography>
            <div className={this.props.classes.listWrapper}>
              {items.folders}
            </div>
          </div>
        )}
        {items.files.length > 0 && (
          <div className={this.props.classes.section}>
            <Typography variant="h6" gutterBottom>
              Files
            </Typography>
            <div className={this.props.classes.listWrapper}>
              {items.files}
            </div>
          </div>
        )}
      </React.Fragment>
    );
  };

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

        {this.props.fileList.length > 0 && !this.props.loading && this.getItemContainer()}

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
            open={this.state.isFileViewerOpen}
            file={this.state.selectedItem}
            onClose={this.onFileViewerClose}
            onDelete={this.onDeleteItem}
          />
        )}

        <ContextMenu deleteHandler={this.onDeleteItem} style={this.state.contextStyles}/>

      </div>
    );
  }
}

export default withStyles(itemListStyles)(connectService(FSContext, ({directories, files, path, loading}, fsService) => ({
  fileList: [...directories, ...files],
  path,
  loading,
  fsService
}))(withRouter(ItemList)));
