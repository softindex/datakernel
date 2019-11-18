const messageFormStyles = theme => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'flex-end',
      flexGrow: 1,
      width: '100%',
      height: 0,
      marginTop: theme.spacing(9)
    },
    wrapper: {
      width: '100%',
    },
    progressWrapper: {
      width: '100%',
      marginTop: theme.spacing(38),
      position: 'relative',
      padding: theme.spacing(10),
      boxSizing: 'border-box',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center'
    },
    fabWrapper: {
      display: 'flex',
      flexDirection: 'row',
      justifyContent: 'center',
      width: '100%',
      margin: '0 auto',
      maxWidth: theme.spacing(113),
      padding: `0 ${theme.spacing(3)}px`
    },
    callingMessage: {
      position: 'absolute',
      top: theme.spacing(9),
      right: theme.spacing(2),
      padding: theme.spacing(),
      backgroundColor: theme.palette.grey[200],
      borderRadius: `${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px 
      ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px`,
      boxShadow: theme.shadows[6]
    },
    message: {
      padding: theme.spacing(2),
      backgroundColor: theme.palette.grey[200],
      borderRadius: `${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px 
      ${theme.shape.borderRadius * 4}px ${theme.shape.borderRadius * 4}px`,
      textAlign: 'center'
    },
    lightColor: {
      color: theme.palette.primary.light
    }
  }
};

export default messageFormStyles;
