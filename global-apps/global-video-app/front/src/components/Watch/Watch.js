import React from 'react';
import connectService from "../../common/connectService";
import IndexContext from "../../modules/index/IndexContext";
import Redirect from "react-router-dom/es/Redirect";
import withStyles from "@material-ui/core/styles/withStyles";
import watchStyles from "./watchStyles";

function Watch({classes, videos, ready, match, publicKey}) {
  if (!ready) {
    return <h1>Loading...</h1>
  }
  const {videoId} = match.params;
  const metadata = videos[videoId];
  if (metadata) {
    return (
      <div className={classes.root}>
        <h1>{metadata.title}</h1>
        <div className={classes.player}>
          <video
            autoPlay={true}
            width='100%'
            height='100%'
            controls={true}
          >
            <source src={`/download/${publicKey}/${videoId}.${metadata.extension}`}
                    type={`video/${metadata.extension}`}/>
          </video>
        </div>
        <p>{metadata.description}</p>
      </div>
    );
  } else {
    return <Redirect to="/"/>
  }
}

export default connectService(IndexContext, ({videos, ready}) => ({videos, ready}))(
  withStyles(watchStyles)(Watch)
);
