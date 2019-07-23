const createChatDialogStyles = theme => ({
  chip: {
    margin: theme.spacing.unit,
    marginTop: 0,
    width: theme.spacing.unit * 18.5,
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
  search: {
    padding: `${theme.spacing.unit *0.25 }px ${theme.spacing.unit *0.5 }px`,
    display: 'flex',
    alignItems: 'center',
    width: 'auto',
    boxShadow: 'none',
    border: '1px solid',
    borderColor: theme.palette.secondary.lightGrey,
    marginTop: theme.spacing.unit,
    marginBottom: theme.spacing.unit
  },
  input: {
    marginLeft: 8,
    flex: 1,
  },
  searchIcon: {
    padding: `${theme.spacing.unit}px ${theme.spacing.unit * 2}px`
  }
});

export default createChatDialogStyles;
