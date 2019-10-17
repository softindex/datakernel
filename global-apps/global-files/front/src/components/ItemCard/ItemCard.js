import React, {Component} from 'react';
import FolderIcon from '@material-ui/icons/FolderOutlined';
import PhotoIcon from '@material-ui/icons/PhotoOutlined';
import AudioIcon from '@material-ui/icons/AudiotrackOutlined';
import VideoIcon from '@material-ui/icons/VideocamOutlined';
import PdfIcon from '@material-ui/icons/PictureAsPdfOutlined';
import TextIcon from '@material-ui/icons/TextFormatOutlined';
import FileIcon from '@material-ui/icons/InsertDriveFileOutlined';
import {withStyles} from "@material-ui/core";
import Card from '@material-ui/core/Card';
import CardActionArea from '@material-ui/core/CardActionArea';
import CardContent from '@material-ui/core/CardContent';
import Typography from '@material-ui/core/Typography';
import {Link} from 'react-router-dom';
import path from 'path';
import {getFileTypeByName} from '../../common/utils';
import itemCardStyles from "./itemCardStyles";

class ItemCard extends Component {

  getIconByType = () => {

    switch (getFileTypeByName(this.props.name)) {
      case 'image':
        return <PhotoIcon className={this.props.classes.fileIcon}/>;
      case 'audio':
        return <AudioIcon className={this.props.classes.fileIcon}/>;
      case 'video':
        return <VideoIcon className={this.props.classes.fileIcon}/>;
      case 'pdf':
        return <PdfIcon className={this.props.classes.fileIcon}/>;
      case 'text':
        return <TextIcon className={this.props.classes.fileIcon}/>;
      default:
        return <FileIcon className={this.props.classes.fileIcon}/>;
    }

  };

  render() {

    if (this.props.isDirectory) {
      return (
        <Card
          className={this.props.classes.root}
          {...this.props}
        >
          <Link to={path.join('/folders', this.props.path, this.props.name)}
                style={{textDecoration: 'none', display: 'block'}}>
            <div className={this.props.classes.folderGroup}>
              <FolderIcon className={this.props.classes.folderIcon}/>
              <Typography noWrap variant="subtitle2">
                {this.props.name}
              </Typography>
            </div>
          </Link>
        </Card>
      );
    }

    return (
      <Card
        onClick={this.props.onClick}
        className={this.props.classes.root}
        {...this.props}
      >
        <CardActionArea>
          <div className={this.props.classes.headerItem}>
            {this.getIconByType()}
          </div>
          <CardContent>
            <Typography noWrap gutterBottom variant="subtitle2">
              {this.props.name}
            </Typography>
          </CardContent>
        </CardActionArea>
      </Card>
    );
  }
}

export default withStyles(itemCardStyles)(ItemCard);
