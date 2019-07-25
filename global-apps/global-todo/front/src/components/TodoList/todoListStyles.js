const todoListStyles = theme => ({
  paper: {
    display: 'flex',
    flexDirection: 'column',
    minWidth: theme.spacing(65),
    boxShadow: `0px ${theme.spacing(1)}px ${theme.spacing(2)}px 0px rgba(0,0,0,0.2)`
  },
  textField: {
    fontSize: 20,
    display: 'flex',
    flexGrow: 1,
    flexShrink: 1,
  },
  iconButton: {
    margin: `0px ${theme.spacing(2)}px`,
    '&:hover': {
      cursor: 'pointer'
    }
  },
  itemInput: {
    padding: `${theme.spacing(2)}px 0px`,
    '& > input': {
      fontSize: '1.2rem'
    },
    '& > fieldset': {
      border: '1px solid',
      borderColor: theme.palette.primary.main
    }
  },
  listCaption: {
    padding: theme.spacing(1),
    display: 'inline-flex'
  },
  captionCounter: {
    margin: theme.spacing(2)
  },
  captionButton: {
    margin: theme.spacing(1),
    color: theme.palette.secondary.grey
  }
});

export default todoListStyles;
