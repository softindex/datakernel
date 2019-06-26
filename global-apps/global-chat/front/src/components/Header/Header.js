import React from 'react';
import {withStyles} from '@material-ui/core';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import headerStyles from './headerStyles';
import Button from "@material-ui/core/Button";
import Profile from "../Profile/Profile";

class Header extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      openProfile: false
    };
  }

  onOpenProfile = () => {
    this.setState({
      openProfile: true
    })
  };

  onCloseProfile = () => {
    this.setState({
      openProfile: false
    })
  };

  render() {
    const {classes} = this.props;
    return (
      <>
        <AppBar className={classes.appBar} position="fixed">
          <Toolbar>
            <Typography color="inherit" variant="h6" className={classes.title}>
              Global Chat
            </Typography>
            <div
              color="inherit"
              className={classes.buttonDiv}
              onClick={this.onOpenProfile}
            >
              <i className="material-icons" style={{fontSize: 36}}>
                account_circle
              </i>
            </div>
          </Toolbar>
        </AppBar>
        <Profile
          open={this.state.openProfile}
          onClose={this.onCloseProfile}
        />
      </>
    );
  }
}

export default withStyles(headerStyles)(Header);
