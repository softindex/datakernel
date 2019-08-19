const chipStyles = theme => ({
  chip: {
    margin: theme.spacing.unit,
    marginBottom: theme.spacing.unit * 2,
    marginTop: 0,
    width: theme.spacing.unit * 14,
    overflow: 'hidden'
  },
  chipText: {
    width: 'inherit',
    overflow: 'hidden',
    display: 'inline-block',
    textOverflow: 'ellipsis'
  }
});

export default chipStyles;
