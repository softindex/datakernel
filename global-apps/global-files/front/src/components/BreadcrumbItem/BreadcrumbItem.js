import React from 'react';
import Button from "@material-ui/core/Button";
import {NavLink} from "react-router-dom";
import Typography from "@material-ui/core/Typography";
import ArrowIcon from '@material-ui/icons/KeyboardArrowRightRounded';
import {withStyles} from "@material-ui/core";
import breadcrumbItemStyles from "./breadcrumbItemStyles";

function BreadcrumbItem({classes, breadcrumb}) {
  return (
    <div className={classes.breadcrumbItem}>
      <Button
        className={classes.button}
        component={NavLink}
        to={breadcrumb.props.match.url}
      >
        <Typography noWrap variant="h6">{breadcrumb}</Typography>
      </Button>
      <ArrowIcon color="inherit" className={classes.icon}/>
    </div>
  );
}

export default withStyles(breadcrumbItemStyles)(BreadcrumbItem);
