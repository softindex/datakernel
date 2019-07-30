const messageFormStyles = theme => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'flex-end',
      flexGrow: 1,
      width: '100%',
      height: 0,
      marginTop: theme.spacing.unit * 9
    },
    messageRow: {
      display: 'flex',
      flexDirection: 'row',
      width: '100%',
      margin: '0 auto',
      maxWidth: theme.spacing.unit * 113,
      padding: `0 ${theme.spacing.unit * 3}px`
    },
    messageRowRightAlign: {
      justifyContent: 'flex-end'
    },
    wrapper: {
      width: '100%',
      overflowY: 'auto',
      '&::-webkit-scrollbar-track': {
        background: 'border-box'
      },
      '&::-webkit-scrollbar-thumb': {
        background: theme.palette.secondary.grey
      }
    },
    message: {
      display: 'flex',
      flexDirection: 'column',
      padding: theme.spacing.unit * 2,
      backgroundColor: theme.palette.grey[200],
      marginBottom: theme.spacing.unit,
      wordBreak: 'break-all',
      borderRadius: `${theme.shape.borderRadius}px ${theme.shape.borderRadius * 4}px 
      ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px`,
    },
    messageFromOther: {
      backgroundColor: theme.palette.grey[100],
      color: theme.palette.grey[100],
      borderRadius: `${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius}px 
      ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px`,
    },
    messageMedium: {
      borderRadius: `${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px
       ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px`
    },
    statusWrapper: {
      marginLeft: theme.spacing.unit,
      color: theme.palette.grey[100]
    },
    progressWrapper: {
      width: '100%',
      marginTop: theme.spacing.unit * 38,
      position: 'relative',
      padding: theme.spacing.unit * 10,
      boxSizing: 'border-box',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center'
    },
    headerPadding: theme.mixins.toolbar,
    paper: {
      borderRadius: 7,
      backgroundColor: theme.palette.primary.darkWhite,
      alignSelf: 'center'
    }
  }
};

export default messageFormStyles;
