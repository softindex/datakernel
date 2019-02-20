const appStyles = theme => {
  return {
    root: {
      backgroundImage: 'linear-gradient(120deg, #e0c3fc 0%, #8ec5fc 100%)',
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'center',
      height: '100vh',
      margin: '0 auto'
    },
    grow: {
      flexGrow: 1
    },
    graphView: {
      position: 'absolute',
      right: theme.spacing.unit * 3,
      top: '100px',
      width: theme.spacing.unit * 15,
      height: theme.spacing.unit * 30,
      backgroundColor: 'grey'
    }
  }
};

export default appStyles;
