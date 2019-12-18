import PhotoIcon from '@material-ui/icons/PhotoOutlined';
import AudioIcon from '@material-ui/icons/AudiotrackOutlined';
import VideoIcon from '@material-ui/icons/VideocamOutlined';
import PdfIcon from '@material-ui/icons/PictureAsPdfOutlined';
import TextIcon from '@material-ui/icons/TextFormatOutlined';
import FileIcon from '@material-ui/icons/InsertDriveFileOutlined';
import React from "react";

function FileTypeIcon({type, className}) {
  switch (type) {
    case 'image':
      return <PhotoIcon className={className}/>;
    case 'audio':
      return <AudioIcon className={className}/>;
    case 'video':
      return <VideoIcon className={className}/>;
    case 'pdf':
      return <PdfIcon className={className}/>;
    case 'text':
      return <TextIcon className={className}/>;
    default:
      return <FileIcon className={className}/>;
  }
}
export default FileTypeIcon;
