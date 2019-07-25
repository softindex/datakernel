const mainScreenStyles = theme => ({
  container: {
    display: 'flex',
    padding: theme.spacing(2),
    minHeight: '100vh',
    alignItems: 'center',
    flexDirection: 'column',
    justifyContent: 'center'
  },
  title: {
    padding: theme.spacing(3)
  },
  caption: {
    padding: theme.spacing(8)
  }
});

export default mainScreenStyles;
