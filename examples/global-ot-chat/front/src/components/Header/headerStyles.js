const headerStyles = theme => ({
  grow: {
    flexGrow: '1'
  },
  graphTriggerButton: {
    [theme.breakpoints.down('sm')]: {
      display: 'none'
    }
  }
});

export default headerStyles;
