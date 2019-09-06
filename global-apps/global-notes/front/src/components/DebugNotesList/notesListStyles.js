const notesListStyles = theme => ({
  notesList: {
    height: '100vh',
    overflowY: 'hidden',
    '&:hover': {
      overflowY: 'overlay'
    },
    '&::-webkit-scrollbar-track': {
      background: 'border-box'
    },
    '&::-webkit-scrollbar-thumb': {
      background: theme.palette.secondary.grey
    }
  },
  progressWrapper: {
    margin: 'auto',
    marginTop: theme.spacing.unit * 3,
    textAlign: 'center'
  },
});

export default notesListStyles;
