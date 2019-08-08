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
  }
});

export default mainScreenStyles;
