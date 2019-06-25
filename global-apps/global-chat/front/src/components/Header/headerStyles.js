const headerStyles = (theme) => ({
  appBar: {
    zIndex: theme.zIndex.drawer + 1,
    height: 65
  },
  title: {
    flexGrow: 1,
    marginLeft: `${theme.spacing.unit * 3}px`
  }
});

export default headerStyles;
