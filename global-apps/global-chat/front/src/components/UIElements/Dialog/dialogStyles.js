const dialogStyles = (theme) => ({
  closeButton: {
    position: 'absolute',
    right: `${theme.spacing.unit*2}px`,
    top: `${theme.spacing.unit}px`,
    color: theme.palette.grey[500]
  },
  circularProgress: {
    position: 'absolute',
    top: 'calc(50% - 12px)',
    left: 'calc(50% - 12px)'
  }
});

export default dialogStyles;
