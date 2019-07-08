import React from 'react';
import {Link} from "react-router-dom";
import {List, ListItemIcon, ListItemText, withStyles} from "@material-ui/core";
import ListItem from "@material-ui/core/ListItem";
import videoListStyles from "./videoListStyles";
import EmptyList from "../EmptyList/EmptyList";
import Paper from "@material-ui/core/Paper";
import connectService from "../../common/connectService";
import IndexContext from "../../modules/index/IndexContext";
import PlayCircle from "@material-ui/icons/PlayCircleFilled";

function VideoList({videos, classes}) {
  return (
    Object.keys(videos).length ?
      <Paper className={classes.list} component={List}>
        {Object.entries(videos).map(([id, {title}]) =>
          <ListItem
            key={id}
            className={classes.listItem}
            button
            component={Link}
            to={`/watch/${id}`}>
            <ListItemIcon>
              <PlayCircle fontSize="large"/>
            </ListItemIcon>
            <ListItemText primary={title}/>
          </ListItem>)}
      </Paper> :
      <EmptyList/>
  );
}

export default connectService(IndexContext, ({ready, videos}) => ({
  ready, videos,
}))(withStyles(videoListStyles)(VideoList));
