const dialogStyles = theme => ({
  closeButton: {
    position: 'absolute',
    right: theme.spacing.unit * 2,
    top: theme.spacing.unit,
    color: theme.palette.grey[500]
  },
  circularProgress: {
    position: 'absolute',
    top: 'calc(50% - 12px)',
    left: 'calc(50% - 12px)',
    zIndex: 3
  },
  muDialog: {
    minWidth: theme.spacing.unit * 47,
    minHeight: theme.spacing.unit * 21
  }
});

export default dialogStyles;
