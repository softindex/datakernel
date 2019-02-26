const messageFormStyles = theme => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'flex-end',
      flexGrow: 1,
      width: '100%',
      height: 'calc(100% - 145px)'
    },
    messageRow: {
      display: 'flex',
      flexDirection: 'row',
      width: '100%',
      margin: '0 auto',
      maxWidth: '900px',
      padding: `0 ${theme.spacing.unit * 3}px`
    },
    messageRowRightAlign: {
      justifyContent: 'flex-end'
    },
    wrapper: {
      width: '100%',
      overflowY: 'auto',
      paddingTop: theme.spacing.unit * 2
    },
    message: {
      display: 'flex',
      flexDirection: 'column',
      padding: theme.spacing.unit * 2,
      backgroundColor: theme.palette.grey[200],
      marginBottom: theme.spacing.unit,
      borderRadius: `${theme.shape.borderRadius}px ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px`,
    },
    messageFromOther: {
      backgroundColor: theme.palette.grey[100],
      color: theme.palette.grey[100],
      borderRadius: `${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius}px ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px`,
    },
    messageMedium: {
      borderRadius: `${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px`
    },
    statusWrapper: {
      marginLeft: theme.spacing.unit,
      color: theme.palette.grey[100]
    },
    progressWrapper: {
      width: '100%',
      padding: theme.spacing.unit * 10,
      boxSizing: 'border-box',
      position: 'absolute',
      top: 0,
      left: 0,
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center'
    },
    headerPadding: theme.mixins.toolbar
  }
};

export default messageFormStyles;
