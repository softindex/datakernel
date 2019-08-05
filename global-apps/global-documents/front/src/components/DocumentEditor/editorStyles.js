const editorStyles = theme => ({
  editor: {
    fontSize: theme.typography.h6.fontSize,
    fontFamily: 'Roboto',
    display: 'flex',
    flexGrow: 1,
    outline: 0,
    resize: 'none',
    borderStyle: 'none',
    background: theme.palette.primary.background,
    padding: theme.spacing.unit * 3,
    paddingTop: theme.spacing.unit * 2,
    marginTop: theme.spacing.unit * 9,
    '&::-webkit-scrollbar-track': {
      background: 'border-box'
    },
    '&::-webkit-scrollbar-thumb': {
      background: theme.palette.secondary.grey
    }
  }
});

export default editorStyles;
