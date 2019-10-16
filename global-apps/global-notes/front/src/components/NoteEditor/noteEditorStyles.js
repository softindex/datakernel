const noteEditorStyles = theme => ({
  noteEditor: {
    fontSize: theme.typography.h6.fontSize,
    fontFamily: 'Roboto',
    display: 'flex',
    flexGrow: 1,
    outline: 0,
    width: '100%',
    height: '100%',
    resize: 'none',
    borderStyle: 'none',
    overflow: 'auto',
    '&:hover': {
      '&::-webkit-scrollbar-thumb': {
        background: theme.palette.secondary.grey
      }
    }
  },
  scroller: {
    scrollbarColor: 'transparent transparent',
    scrollbarWidth: 'thin',
    '&:hover': {
      scrollbarColor: `${theme.palette.secondary.grey} transparent`,
    }
  },
  paper: {
    flexGrow: 1,
    margin: theme.spacing(3),
    marginTop: theme.spacing(11),
    padding: theme.spacing(3),
    paddingRight: theme.spacing(2)
  },
  circularProgress: {
    margin: 'auto'
  }
});

export default noteEditorStyles;
