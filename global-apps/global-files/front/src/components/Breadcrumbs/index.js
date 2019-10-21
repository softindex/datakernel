import React, {useState} from 'react';
import withBreadcrumbs from 'react-router-breadcrumbs-hoc';
import Typography from '@material-ui/core/Typography';
import {withStyles} from "@material-ui/core";
import ArrowBackIcon from '@material-ui/icons/ArrowBackIosOutlined';
import IconButton from '@material-ui/core/IconButton';
import Button from '@material-ui/core/Button';
import withWidth, {isWidthUp, isWidthDown} from '@material-ui/core/withWidth';
import DropDownIcon from '@material-ui/icons/ArrowDropDownOutlined';
import breadcrumbsStyles from './breadcrumbsStyles';
import BreadcrumbsMenu from '../BreadcrumbsMenu';
import BreadcrumbItem from "../BreadcrumbItem";
import {withSnackbar} from "notistack";
import FSService from "../../modules/fs/FSService";
import {getInstance} from "global-apps-common";

function BreadcrumbsView({
                           classes,
                           breadcrumbs,
                           location,
                           history,
                           width,
                           anchorElement,
                           onRemoveDir,
                           onContextMenuClose,
                           onOpenContextMenu
                         }) {
  let crumbs = breadcrumbs.map((breadcrumb, index) => {
    if (breadcrumb.props.match.url === location.pathname) {
      return (
        <>
          {isWidthDown('sm', width) && index > 0 && (
            <IconButton
              onClick={() => history.push(breadcrumbs[index - 1].key)}
              color="inherit">
              <ArrowBackIcon/>
            </IconButton>
          )}
          <div className={classes.breadcrumbLastItem}>
            <Button
              onClick={onOpenContextMenu}
              className={classes.button}
              disabled={breadcrumbs.length === 1}
            >
              <Typography noWrap variant="h6">{breadcrumb}</Typography>
              {breadcrumbs.length > 1 && (
                <DropDownIcon
                  color="inherit"
                  className={classes.dropDownIcon}
                />
              )}
            </Button>
            {breadcrumbs.length > 1 && (
              <BreadcrumbsMenu
                anchorElement={anchorElement}
                onDelete={onRemoveDir.bind(null, breadcrumbs[index - 1])}
                onClose={onContextMenuClose}
              />
            )}
          </div>
        </>
      );
    }
    return <BreadcrumbItem breadcrumb={breadcrumb}/>;
  });

  if (!isWidthUp('sm', width)) {
    crumbs = crumbs.slice(-1);
  }

  return (
    <div className={classes.root}>
      {crumbs}
    </div>
  )
}

function Breadcrumbs({classes, width, enqueueSnackbar, breadcrumbs, location, history}) {
  const fsService = getInstance(FSService);
  const [anchorElement, setAnchorElement] = useState(null);

  const props = {
    classes,
    width,
    breadcrumbs,
    location,
    history,
    anchorElement,

    onRemoveDir(prevCrumb) {
      fsService.rmdir()
        .then(() => {
          setAnchorElement(null);
          history.push(prevCrumb.key);
        })
        .catch((err) => {
          enqueueSnackbar(err.message, {
            variant: 'error'
          });
        });
    },

    onOpenContextMenu(e) {
      setAnchorElement(e.currentTarget);
    },

    onContextMenuClose() {
      setAnchorElement(null);
    }
  };

  return <BreadcrumbsView {...props}/>
}

export default withWidth()(
  withSnackbar(
    withStyles(breadcrumbsStyles)(
      withBreadcrumbs(
        [{path: '/folders', breadcrumb: 'My files'}],
        {excludePaths: ['/']}
      )(Breadcrumbs)
    )
  )
);
