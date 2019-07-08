import React, {Component} from 'react';
import IndexContext from "../../modules/index/IndexContext";
import IndexService from "../../modules/index/IndexService";
import Watch from "../Watch/Watch";
import connectService from "../../common/connectService";
import AuthContext from "../../modules/auth/AuthContext";
import checkAuth from "../HOC/checkAuth";
import Header from "../Header/Header";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import {Route} from "react-router-dom";
import VideoList from "../VideoList/VideoList";
import Upload from "../Upload/Upload";

class MainScreen extends Component {
  constructor(props) {
    super(props);
    this.indexService = IndexService.create();
  }

  componentDidMount() {
    this.indexService.init();
  }

  componentWillUnmount() {
    this.indexService.stop();
  }

  render() {
    return (
      <IndexContext.Provider value={this.indexService}>
        <Header history={this.props.history}/>
        <div className={this.props.classes.screen}>
          <Route
            exact={true}
            path="/watch/:videoId"
            render={(props) => <Watch {...props} publicKey={this.props.publicKey}/>}
          />
          <Route
            exact={true}
            path="/upload"
            render={(props) => <Upload {...props} publicKey={this.props.publicKey}/>}/>
          <Route exact={true} path="/" component={VideoList}/>
        </div>
      </IndexContext.Provider>
    )
  }
}

export default connectService(AuthContext, ({publicKey}) => ({publicKey}))(
  checkAuth(
    withStyles(mainScreenStyles)(MainScreen)
  )
);
