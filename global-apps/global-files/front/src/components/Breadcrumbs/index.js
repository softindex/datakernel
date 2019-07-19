import React from 'react';
import withBreadcrumbs from 'react-router-breadcrumbs-hoc';
import {NavLink} from "react-router-dom";
import Typography from '@material-ui/core/Typography';
import {withStyles} from "@material-ui/core";
import ArrowIcon from '@material-ui/icons/KeyboardArrowRightRounded';
import ArrowBackIcon from '@material-ui/icons/ArrowBackIosOutlined';
import DeleteIcon from '@material-ui/icons/DeleteForeverOutlined';
import IconButton from '@material-ui/core/IconButton';
import Button from '@material-ui/core/Button';
import withWidth, {isWidthUp, isWidthDown} from '@material-ui/core/withWidth';
import DropDownIcon from '@material-ui/icons/ArrowDropDownOutlined';
import breadcrumbsStyles from './BreadcrumbsStyles';
import MenuList from '@material-ui/core/MenuList';
import MenuItem from '@material-ui/core/MenuItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import Menu from '@material-ui/core/Menu';

class ContextMenu extends React.Component {
  render() {
    return (
      <Menu
        open={Boolean(this.props.anchorElement)}
        anchorEl={this.props.anchorElement}
        onClose={this.props.onClose}
      >
        <MenuList>
          <MenuItem button onClick={this.props.onDelete}>
            <ListItemIcon>
              <DeleteIcon/>
            </ListItemIcon>
            <ListItemText inset primary="Delete"/>
          </MenuItem>
        </MenuList>
      </Menu>
    )
  }
}

class Breadcrumbs extends React.Component {
  state = {
    anchorElement: null
  };

  onRemoveDir = async (prevCrumb) => {
    try {
      await this.props.fsService.rmdir();
    } catch (e) {
      alert(e.message);
    }

    this.setState({
      anchorElement: null
    });

    this.props.history.push(prevCrumb.key);
  };

  contextMenuHandler = async (e) => {
    this.setState({
      anchorElement: e.currentTarget
    });
  };

  onContextMenuClose = () => {
    this.setState({
      anchorElement: null
    });
  };

  render() {
    let crumbs = this.props.breadcrumbs.map((breadcrumb, index) => {
      if (breadcrumb.props.match.url === this.props.location.pathname) {
        return (
          <React.Fragment>
            {isWidthDown('sm', this.props.width) && index > 0 && (
              <IconButton
                onClick={() => this.props.history.push(this.props.breadcrumbs[index - 1].key)}
                color="inherit">
                <ArrowBackIcon/>
              </IconButton>
            )}
            <div className={this.props.classes.breadcrumbItem}>
              <Button
                onClick={this.contextMenuHandler}
                className={this.props.classes.button}
                disabled={this.props.breadcrumbs.length === 1}
              >
                <Typography noWrap variant="h6">{breadcrumb}</Typography>
                {this.props.breadcrumbs.length > 1 && (
                  <DropDownIcon
                    color="inherit"
                    className={this.props.classes.dropDownIcon}
                  />
                )}
              </Button>
              {this.props.breadcrumbs.length > 1 && (
                <ContextMenu
                  anchorElement={this.state.anchorElement}
                  onDelete={this.onRemoveDir.bind(null, this.props.breadcrumbs[index - 1])}
                  onClose={this.onContextMenuClose}
                />
              )}
            </div>
          </React.Fragment>
        );
      }

      return (
        <div className={this.props.classes.breadcrumbItem}>
          <Button
            className={this.props.classes.button}
            component={NavLink}
            to={breadcrumb.props.match.url}
          >
            <Typography noWrap variant="h6">{breadcrumb}</Typography>
          </Button>
          <ArrowIcon color="inherit" className={this.props.classes.icon}/>
        </div>
      );
    });

    if (!isWidthUp('sm', this.props.width)) {
      crumbs = crumbs.slice(-1);
    }

    return (
      <div className={this.props.classes.root}>
        {crumbs}
      </div>
    )
  }

}

export default withWidth()(withStyles(breadcrumbsStyles)(
  withBreadcrumbs(
    [{path: '/folders', breadcrumb: 'My files'}],
    {excludePaths: ['/']}
  )(Breadcrumbs))
);
