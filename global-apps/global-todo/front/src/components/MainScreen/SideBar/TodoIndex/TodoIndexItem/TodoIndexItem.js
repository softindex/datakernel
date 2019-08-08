import React from "react";
import {withStyles} from '@material-ui/core';
import ListItemText from "@material-ui/core/ListItemText";
import ListItem from "@material-ui/core/ListItem";
import todoIndexItemStyles from "./todoIndexItemStyles";
import {Link, withRouter} from "react-router-dom";
import SimpleMenu from "../../SimpleMenu/SimpleMenu";

class TodoIndexItem extends React.Component {
  state = {hover: false};

  toggleHover = () => {
    this.setState({hover: !this.state.hover})
  };

  render() {
    const {classes, listId, listName} = this.props;
    return (
      <>
        <ListItem
          onMouseEnter={this.toggleHover}
          onMouseLeave={this.toggleHover}
          className={classes.listItem}
          button
          selected={listId === this.props.match.params.listId}
        >
          <Link
            to={this.props.getListPath(listId)}
            className={classes.link}
          >
            <ListItemText
              primary={listName}
              className={classes.itemText}
              classes={{
                primary: classes.itemTextPrimary
              }}
            />
          </Link>
          <SimpleMenu
            className={classes.menu}
            onRename={() => this.props.onRename(listId, listName)}
            onDelete={() => this.props.onDelete(listId)}
          />
        </ListItem>
      </>
    )
  }
}

export default withRouter(withStyles(todoIndexItemStyles)(TodoIndexItem));

