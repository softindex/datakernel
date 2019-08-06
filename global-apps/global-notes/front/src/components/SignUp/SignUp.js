import React from 'react';
import {withStyles} from '@material-ui/core';
import StoreIcon from '@material-ui/icons/Store';
import AttachFileIcon from '@material-ui/icons/AttachFile';
import Typography from '@material-ui/core/Typography';
import Grid from '@material-ui/core/Grid';
import Button from '@material-ui/core/Button';
import connectService from '../../common/connectService';
import AccountContext from '../../modules/account/AccountContext';
import signUpStyles from './signUpStyles';
import SignUpAbstractionImage from '../SignUpAbstractionImage/SignUpAbstractionImage';

class SignUp extends React.Component {
  state = {
    online: window.navigator.onLine
  };

  onAuthByAppStore = async () => {
    await this.props.accountService.authWithAppStore();
  };

  onUploadFile = () => {
    this.props.accountService.authByFile(this.input.files[0]).then(() => {
      this.props.history.push('/');
    });
  };

  componentDidMount() {
    window.addEventListener('online', this._wentOnline);
    window.addEventListener('offline', this._wentOffline);
  }

  componentWillUnmount() {
    window.removeEventListener('online', this._wentOnline);
    window.removeEventListener('offline', this._wentOffline);
  }

  _wentOnline = () => {
    this.setState({
      online: true
    });
  };

  _wentOffline = () => {
    this.setState({
      online: false
    });
  };

  render() {
    const {classes} = this.props;

    return (
      <div className={classes.root}>
        <Grid className={classes.container} container>
          <Grid className={classes.column} item xs={12} sm={12} md={8} lg={6}>
            <Typography
              variant="h2"
              gutterBottom
              color="textPrimary"
              className={classes.title}
            >
              Global Notes
            </Typography>
            <Typography
              className={classes.description}
              variant="h6"
              color="textSecondary"
            >
              An application that allows you to keep notes. It is distributed and supports synchronization
              across all your devices.
            </Typography>
            <Grid container spacing={32}>
              <Grid item xs={12} lg={6} md={6}>
                <Button
                  variant="contained"
                  color="primary"
                  className={classes.button}
                  shape="round"
                  fullWidth
                  disabled={!this.state.online}
                  onClick={this.onAuthByAppStore}
                >
                  <StoreIcon className={classes.storeIcon}/>
                  Auth by App Store
                </Button>
              </Grid>
              <Grid item xs={12} lg={6} md={6}>
                <Button
                  variant="outlined"
                  color="inherit"
                  className={classes.button}
                  shape="round"
                  fullWidth
                  onClick={() => this.input.click()}
                >
                  <AttachFileIcon className={classes.attachIcon}/>
                  Auth by key
                </Button>
              </Grid>
            </Grid>
          </Grid>
          <Grid className={classes.columnRight} item md={4} lg={6}>
            <div className={classes.gradientOverlay}/>
            <SignUpAbstractionImage
              nodesCount={30}
              nodesSize={2}
              size={100}
              className={classes.animation}
            />
          </Grid>
        </Grid>
        <input
          accept=".dat"
          ref={ref => this.input = ref}
          type="file"
          className={classes.input}
          onChange={this.onUploadFile}
        />
      </div>
    );
  }

}

export default connectService(
  AccountContext,
  ({authorized, loading}, accountService) => ({authorized, loading, accountService})
)(
  withStyles(signUpStyles)(SignUp)
);

