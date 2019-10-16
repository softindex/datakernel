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
      overflow: 'hidden',
      '&:hover': {
        overflow: 'overlay'
      }
    },
    scroller: {
      scrollbarColor: 'transparent transparent',
      scrollbarWidth: 'thin',
      '&:hover': {
        scrollbarColor: `${theme.palette.secondary.grey} transparent`,
      }
    },
    '@supports ( -moz-appearance:none )': {
      wrapper: {
        overflow: 'auto'
      }
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
    }
  }
};

export default messageFormStyles;
