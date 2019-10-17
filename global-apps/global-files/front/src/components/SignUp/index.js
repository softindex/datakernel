import React from 'react';
import AuthContext from '../../modules/auth/AuthContext';
import connectService from '../../common/connectService';
import Typography from '@material-ui/core/Typography';
import {withStyles} from '@material-ui/core/styles';
import AttachFileIcon from '@material-ui/icons/AttachFile';
import StoreIcon from '@material-ui/icons/Store';
import Grid from '@material-ui/core/Grid';
import {SignUpAbstractionImage} from 'global-apps-common';
import Button from '../Button/Button';
import Snackbar from '../Snackbar/Snackbar';
import signUpStyles from './signUpStyles';

class SignUp extends React.Component {
  constructor(props) {
    super(props);
    this.state = {online: window.navigator.onLine};
    this._wentOnline = () => this.setState({online: true});
    this._wentOffline = () => this.setState({online: false});
  }

  onAuthByAppStore = () => {
    this.props.authService.authWithAppStore();
  };

  componentDidMount() {
    window.addEventListener('online', this._wentOnline);
    window.addEventListener('offline', this._wentOffline);
  }

  componentWillUnmount() {
    window.removeEventListener('online', this._wentOnline);
    window.removeEventListener('offline', this._wentOffline);
  }

  onUploadFile = () => {
    this.props.authService.authByFile(this.input.files[0]).then(() => {
      this.props.history.push('/');
    });
  };

  render() {
    return (
      <div className={this.props.classes.root}>
        <Grid className={this.props.classes.container} container>
          <Grid className={this.props.classes.column} item xs={12} sm={12} md={8} lg={6}>
            <Typography
              variant="h2"
              gutterBottom
              color="textPrimary"
              className={this.props.classes.title}
            >
              Global Files
            </Typography>
            <Typography
              className={this.props.classes.description}
              variant="h6"
              color="textSecondary"
            >
              An application that allows you to organize a single workspace.
              They are easy to manage, and synchronize themselves on all devices.
            </Typography>
            <Grid container spacing={32}>
              <Grid item xs={12} lg={6} md={6}>
                <Button
                  variant="outlined"
                  color="inherit"
                  className={this.props.classes.button}
                  shape="round"
                  fullWidth
                  disabled={!this.state.online}
                  onClick={this.onAuthByAppStore}
                >
                  <StoreIcon className={this.props.classes.storeIcon}/>
                  Auth by App Store
                </Button>
              </Grid>
              <Grid item xs={12} lg={6} md={6}>
                <Button
                  variant="outlined"
                  color="inherit"
                  className={this.props.classes.button}
                  shape="round"
                  fullWidth
                  onClick={() => this.input.click()}
                >
                  <AttachFileIcon className={this.props.classes.attachIcon}/>
                  Auth by key
                </Button>
              </Grid>
            </Grid>
          </Grid>
          <Grid className={this.props.classes.columnRight} item md={4} lg={6}>
            <div className={this.props.classes.gradientOverlay}/>
            <SignUpAbstractionImage
              nodesCount={30}
              nodesSize={2}
              size={100}
              className={this.props.classes.animation}
            />
          </Grid>
        </Grid>
        <Snackbar
          error={this.props.auth.error && this.props.auth.error.message}
          action={[
            <Button key="undo" color="secondary" size="small" onClick={this.onAuthByAppStore}>
              RETRY
            </Button>,
          ]}
        />
        <input
          accept=".dat"
          ref={ref => this.input = ref}
          type="file"
          className={this.props.classes.input}
          onChange={this.onUploadFile}
        />
      </div>
    );
  }
}

export default withStyles(signUpStyles)(
  connectService(AuthContext, (auth, authService) => ({auth, authService}))(SignUp)
);
