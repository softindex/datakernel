const sideBarStyles = theme => ({
  wrapper: {
    boxShadow: `2px 0px 1px -2px rgba(0,0,0,0.2)`,
    background: theme.palette.primary.contrastText,
    width: 350,
    height: '100vh',
    display: 'flex',
    flexDirection: 'column',
    flexGrow: 0,
    flexShrink: 0,
    position: 'static'
  },
  notesList: {
    flexGrow: 1,
    overflowY: 'auto',
    overflowX: 'hidden',
    background: theme.palette.primary.contrastText,
    marginTop: theme.spacing.unit,
    marginBottom: theme.spacing.unit,
    '&::-webkit-scrollbar-track': {
      background: 'border-box'
    },
    '&::-webkit-scrollbar-thumb': {
      background: theme.palette.secondary.grey
    }
  },
  button: {
    width: theme.spacing.unit * 41,
    margin: 'auto',
    borderRadius: theme.spacing.unit * 9,
    marginBottom: theme.spacing.unit,
    marginTop: theme.spacing.unit * 11
  }
});

export default sideBarStyles;
