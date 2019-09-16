import React from 'react';
import ChatRoom from "../ChatRoom/ChatRoom"
import SideBar from "../SideBar/SideBar";
import {withStyles} from '@material-ui/core';
import mainScreenStyles from "./mainScreenStyles";
import {AuthContext, connectService} from 'global-apps-common';
import MainLayout from "../MainLayout/MainLayout";
import EmptyChatScreen from "../EmptyChatScreen/EmptyChatScreen";

function MainScreen({classes, publicKey, match}) {
  const {roomId} = match.params;
  return (
    <MainLayout>
      <div className={classes.chat}>
        <SideBar publicKey={publicKey}/>
        {!roomId && (
          <EmptyChatScreen/>
        )}
        {roomId && (
          <ChatRoom roomId={roomId} publicKey={publicKey}/>
        )}
      </div>
    </MainLayout>
  );
}

export default connectService(
  AuthContext, ({publicKey}) => ({publicKey})
)(
  withStyles(mainScreenStyles)(MainScreen)
);