import Typography from "@material-ui/core/Typography";
import React from "react";
import {getItemsByType} from "./getItemsByType";

export const getItemContainer = (path, fileList, openFileViewer, handleContextMenu, classes) => {
  const items = getItemsByType(path, fileList, openFileViewer, handleContextMenu);

  return (
    <>
      {items.folders.length > 0 && (
        <div className={classes.section}>
          <Typography variant="h6" gutterBottom>
            Folders
          </Typography>
          <div className={classes.listWrapper}>
            {items.folders}
          </div>
        </div>
      )}
      {items.files.length > 0 && (
        <div className={classes.section}>
          <Typography variant="h6" gutterBottom>
            Files
          </Typography>
          <div className={classes.listWrapper}>
            {items.files}
          </div>
        </div>
      )}
    </>
  );
};

