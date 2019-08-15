const notesListStyles = theme => ({
  notesList: {
    height: '100vh',
    overflowY: 'hidden',
    '&:hover': {
      overflowY: 'auto'
    },
    '&::-webkit-scrollbar-track': {
      background: 'border-box'
    },
    '&::-webkit-scrollbar-thumb': {
      background: theme.palette.secondary.grey
    }
  },
  progressWrapper: {
    marginLeft: theme.spacing.unit * 19,
    marginTop: theme.spacing.unit
  }
});

export default notesListStyles;
