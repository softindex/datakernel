const formStyles = theme => ({
  chip: {
    margin: theme.spacing.unit*1,
    marginTop: 0,
    width: 148,
    overflow: 'hidden'
  },
  actionButton: {
    margin: theme.spacing,
    position: 'relative',
    right: theme.spacing.unit*2
  },
  chipsContainer: {
    display: 'flex',
    flexFlow: 'row wrap',
    maxWidth: 350
  },
  chipText: {
    width: 'inherit',
    overflow: 'hidden',
    display: 'inline-block',
    textOverflow: 'ellipsis'
  },
  root: {
    padding: '2px 4px',
    display: 'flex',
    alignItems: 'center',
    width: 'auto',
    marginTop: theme.spacing.unit,
    marginBottom: theme.spacing.unit
  },
  input: {
    marginLeft: 8,
    flex: 1,
  },
  iconButton: {
    padding: 10,
  }
});

export default formStyles;
