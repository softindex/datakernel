import ItemCard from "../ItemCard/ItemCard";
import React from "react";

export const getItemsByType = (path, fileList, openFileViewer, handleContextMenu) => {
  let folders = [];
  let files = [];

  for (const item of fileList) {
    if (item.isDirectory) {
      folders.push(
        <ItemCard
          name={item.name}
          isDirectory={true}
          filePath={path}
          onContextMenu={handleContextMenu.bind(null, item.name, item.isDirectory)}
        />
      )
    } else {
      files.push(
        <ItemCard
          name={item.name}
          isDirectory={false}
          onClick={openFileViewer.bind(null, item)}
          onContextMenu={handleContextMenu.bind(null, item.name, item.isDirectory)}
        />
      )
    }
  }

  return {
    folders,
    files
  }
};
