const formStyles = theme => ({
  closeButton: {
    position: 'absolute',
    right: `${theme.spacing.unit*2}px`,
    top: `${theme.spacing.unit}px`,
    color: theme.palette.grey[500]
  },
  chip: {
    margin: `${theme.spacing.unit*1}px`,
    marginTop: 0
  },
  progressButton: {
    right: `${theme.spacing.unit*2}px`
  }
});

export default formStyles;
