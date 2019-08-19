const searchStyles = theme => ({
  inputDiv: {
    marginLeft: theme.spacing.unit,
    marginRight: theme.spacing.unit * 3,
    flex: 1
  },
  input: {
    textOverflow: 'ellipsis',
    overflow: 'hidden',
    whiteSpace: 'nowrap'
  },
  progressWrapper: {
    display: 'flex'
  },
  iconButton: {
    padding: `${theme.spacing.unit}px ${theme.spacing.unit}px`
  }
});

export default searchStyles;
