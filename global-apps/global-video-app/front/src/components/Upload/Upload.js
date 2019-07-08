import React, {Component} from 'react';
import connectService from "../../common/connectService";
import GlobalFS from "../../common/GlobalFS";
import {Button, LinearProgress, withStyles} from "@material-ui/core";
import Redirect from "react-router-dom/es/Redirect";
import IndexContext from "../../modules/index/IndexContext";
import uploadStyles from "./uploadStyles";
import TextField from "@material-ui/core/TextField";
import Wrapper from "../../common/Wrapper";

class Upload extends Component {
  constructor(props, context) {
    super(props, context);
    this.fs = new GlobalFS(props.publicKey);
    this.state = {
      title: '',
      description: '',
      file: null,
      uploadError: null,
      progress: -1,
      id: null,
    }
  }

  componentDidMount() {
    this.fs.addListener('progress', this.updateProgress)
  }

  componentWillUnmount() {
    this.fs.removeListener('progress', this.updateProgress);
  }

  updateProgress = obj => this.setState({progress: obj.progress});

  uploadFile = e => {
    e.preventDefault();
    const {title, description, file} = this.state;

    const id = Math.random().toString(36).substring(2);
    const extension = this.state.file.name.split('.').pop();
    if (!extension) {
      throw new Error('File should have an extension');
    }
    const fileName = `${id}.${extension}`;
    return this.fs.upload(fileName, file)
      .then(() => this.props.addVideo(id, title, description, extension))
      .catch(e => {
        console.error(e);
        return this.setState({progress: -1, uploadError: e});
      })
      .finally(() => this.setState({id}));
  };

  onChange = e => {
    this.setState({[e.target.name]: e.target.value});
  };

  onFileChange = e => {
    this.setState({
      file: e.target.files[0]
    });
  };

  render() {
    const {progress, id, file} = this.state;
    const {classes} = this.props;
    const loadInProgress = progress !== -1;
    if (id) {
      return <Redirect to={`/watch/${id}`}/>;
    }
    return (
      <div>
        <form className={classes.form} onSubmit={this.uploadFile}>
          <Wrapper disabled={loadInProgress}>
            <TextField
              variant="outlined"
              margin="normal"
              required
              fullWidth
              label="Title"
              name="title"
              autoComplete="title"
              onChange={this.onChange}
              autoFocus
            />
            <TextField
              variant="outlined"
              margin="normal"
              fullWidth
              rows={10}
              multiline={true}
              label="Description"
              name="description"
              autoComplete="description"
              onChange={this.onChange}
            />
            <h2>{file ? file.name : ''}</h2>
            <input
              type="file"
              className={classes.uploadInput}
              accept=".mp4"
              onChange={this.onFileChange}
              ref={ref => this.input = ref}
            />
            <Button
              fullWidth
              variant="contained"
              color="primary"
              className={classes.button}
              onClick={() => this.input.click()}
            >
              Select file
            </Button>
          </Wrapper>
          <Button
            disabled={!file || loadInProgress}
            type="submit"
            fullWidth
            variant="contained"
            color="primary"
            className={classes.button}
          >
            Upload
          </Button>
        </form>
        {progress !== -1 &&
        (<LinearProgress
          variant="buffer"
          value={progress}
          valueBuffer={100}
        />)}
      </div>
    );
  }
}

export default connectService(IndexContext, (state, indexService) => ({
  addVideo: (...args) => indexService.add(...args)
}))(withStyles(uploadStyles)(Upload));
