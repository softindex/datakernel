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
    progressWrapper: {
      width: '100%',
      marginTop: theme.spacing.unit * 38,
      position: 'relative',
      padding: theme.spacing.unit * 10,
      boxSizing: 'border-box',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center'
    }
  }
};

export default messageFormStyles;
