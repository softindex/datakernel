const fileViewerStyles = theme => ({
  root: {
    display: 'flex',
    flexDirection: 'column'
  },
  appBar: {
    boxShadow: 'none'
  },
  image: {
    maxWidth: '100%',
    maxHeight: '100%',
    height: 'auto',
    boxShadow: '0px 6px 6px -3px rgba(0,0,0,0.2), 0px 10px 14px 1px rgba(0,0,0,0.14), 0px 4px 18px 3px rgba(0,0,0,0.12);'
  },
  closeIcon: {
    marginRight: theme.spacing(2)
  },
  content: {
    flexGrow: 1,
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    marginRight: theme.spacing(2),
    marginLeft: theme.spacing(2),
    overflow: 'hidden'
  },
  grow: {
    flexGrow: 1
  },
  icon: {
    marginLeft: theme.spacing(1)
  },
  title: {
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis'
  },
  noPreviewContainer: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    flexDirection: 'column',
    padding: theme.spacing(2)
  },
  downloadIcon: {
    marginRight: theme.spacing(2)
  }
});

export default fileViewerStyles;
