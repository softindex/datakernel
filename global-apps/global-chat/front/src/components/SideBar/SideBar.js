import React from "react";
import Drawer from '@material-ui/core/Drawer';
import {withStyles} from '@material-ui/core';
import sideBarStyles from "./sideBarStyles";
import TabBar from "./TabBar/TabBar";

class SideBar extends React.Component {
  render() {
    const {classes} = this.props;
    return (
      <div className={classes.sideBar}>
        <Drawer
          className={classes.drawer}
          variant="permanent"
          classes={{
            paper: classes.drawerPaper,
          }}
        >
          <div className={classes.toolbar}/>
          <TabBar/>
        </Drawer>
      </div>
    )
  }
}

export default withStyles(sideBarStyles)(SideBar);

