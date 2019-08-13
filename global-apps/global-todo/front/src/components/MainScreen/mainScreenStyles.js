const mainScreenStyles = theme => ({
  container: {
    display: 'flex',
    minHeight: '100vh',
    padding: 0,
    alignItems: 'center',
    flexDirection: 'column',
    justifyContent: 'center'
  },
  title: {
    padding: theme.spacing(4.5)
  },
  caption: {
    padding: theme.spacing(8)
  },
  logoutIcon: {
    fontSize: theme.spacing(3.5)
  },
  logout: {
    position: 'fixed',
    top: theme.spacing(2),
    right: theme.spacing(3),
    '&:hover': {
      cursor: 'pointer'
    }
  }
});

export default mainScreenStyles;
