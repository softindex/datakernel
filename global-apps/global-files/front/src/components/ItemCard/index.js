import React from 'react';
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

function ItemCard({classes, filePath, isDirectory, onClick, name}) {
  const getIconByType = () => {
    switch (getFileTypeByName(name)) {
      case 'image':
        return <PhotoIcon className={classes.fileIcon}/>;
      case 'audio':
        return <AudioIcon className={classes.fileIcon}/>;
      case 'video':
        return <VideoIcon className={classes.fileIcon}/>;
      case 'pdf':
        return <PdfIcon className={classes.fileIcon}/>;
      case 'text':
        return <TextIcon className={classes.fileIcon}/>;
      default:
        return <FileIcon className={classes.fileIcon}/>;
    }
  };

  return (
    <>
      {isDirectory && (
        <Card className={classes.root}>
          <Link
            to={path.join('/folders', filePath, name)}
            className={classes.foldersLink}
          >
            <div className={classes.folderGroup}>
              <FolderIcon className={classes.folderIcon}/>
              <Typography noWrap variant="subtitle2">
                {name}
              </Typography>
            </div>
          </Link>
        </Card>
      )}
      {!isDirectory && (
        <Card
          onClick={onClick}
          className={classes.root}
        >
          <CardActionArea>
            <div className={classes.headerItem}>
              {getIconByType()}
            </div>
            <CardContent>
              <Typography
                noWrap
                gutterBottom
                variant="subtitle2"
              >
                {name}
              </Typography>
            </CardContent>
          </CardActionArea>
        </Card>
      )}
    </>
  );
}

export default withStyles(itemCardStyles)(ItemCard);
