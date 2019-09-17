const mainLayoutStyles = theme => ({
  container: {
    display: 'flex',
    minHeight: '100vh',
    padding: 0,
    alignItems: 'center',
    flexDirection: 'column',
    justifyContent: 'center'
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

export default mainLayoutStyles;
