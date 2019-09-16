const dialogStyles = theme => ({
  closeButton: {
    position: 'absolute',
    right: theme.spacing(2),
    top: theme.spacing(1),
    color: theme.palette.grey[500]
  },
  circularProgress: {
    position: 'absolute',
    top: 'calc(50% - 12px)',
    left: 'calc(50% - 12px)',
    zIndex: 3
  },
  muDialog: {
    minWidth: theme.spacing(47),
    minHeight: theme.spacing(21)
  }
});

export default dialogStyles;
