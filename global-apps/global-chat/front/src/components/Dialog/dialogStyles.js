const dialogStyles = (theme) => ({
  closeButton: {
    position: 'absolute',
    right: theme.spacing.unit * 2,
    top: theme.spacing.unit,
    color: theme.palette.grey[500]
  },
  circularProgress: {
    position: 'absolute',
    top: 'calc(50% - 12px)',
    left: 'calc(50% - 12px)'
  },
  muDialog: {
    minWidth: theme.spacing.unit * 47
  }
});

export default dialogStyles;
