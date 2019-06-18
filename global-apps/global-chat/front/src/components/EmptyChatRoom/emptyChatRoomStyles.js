const emptyChatRoomStyles = theme => {
  return {
    root: {
      display: 'flex',
      flexDirection: 'column',
      flexGrow: 1,
      height: '100vh',
      alignItems: 'center',
      justifyContent: 'center'
    },
    headerPadding: theme.mixins.toolbar,
    typography: {
      color: theme.palette.primary.contrastText,
    },
    paper: {
      display: 'flex',
      backgroundColor: '#808080',
      padding: 6,
      paddingLeft: 12,
      paddingRight: 12,
      borderRadius: '30px',
      boxShadow: 'none'
    },
    startMessage: {
      fontSize: '1rem',
      textAlign: 'center',
    }
  }
};

export default emptyChatRoomStyles;
