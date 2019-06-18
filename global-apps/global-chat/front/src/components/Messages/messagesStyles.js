const messageFormStyles = theme => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'flex-end',
      flexGrow: 1,
      width: '100%',
      height: 'calc(100% - 1px)',
      marginTop: '-150px'
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
      paddingTop: theme.spacing.unit * 2,
      marginTop: 165,
      '&::-webkit-scrollbar-track': {
        background: 'border-box'
      },
      '&::-webkit-scrollbar-thumb': {
        background: '#66666680'
      }
    },
    message: {
      display: 'flex',
      flexDirection: 'column',
      padding: theme.spacing.unit * 2,
      backgroundColor: theme.palette.grey[200],
      marginBottom: theme.spacing.unit,
      wordBreak: 'break-all',
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
      marginTop: 300,
      position: 'relative',
      padding: theme.spacing.unit * 10,
      boxSizing: 'border-box',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center'
    },
    headerPadding: theme.mixins.toolbar,
    paper: {
      borderRadius: '7px',
      backgroundColor: '#F5F5DC',
      alignSelf: 'center'
    }
  }
};

export default messageFormStyles;
