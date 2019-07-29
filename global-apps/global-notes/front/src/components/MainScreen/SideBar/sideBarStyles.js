const sideBarStyles = theme => ({
  wrapper: {
    borderRight: '2px solid #DCDCDC',
    marginTop: theme.spacing.unit * 8,
    width: theme.spacing.unit * 44,
    flexGrow: 0,
    flexShrink: 0,
    position: 'static'
  },
  tabContent: {
    height: 'calc(100% - 115px)',
    position: 'fixed',
    padding: theme.spacing.unit * 1.5
  },
  notesList: {
    height: 'calc(100% - 240px)',
    position: 'fixed',
    overflowY: 'auto',
    '&::-webkit-scrollbar-track': {
      background: 'border-box'
    },
    '&::-webkit-scrollbar-thumb': {
      background: theme.palette.grey[700]
    }
  },
  button: {
    width: theme.spacing.unit * 41,
    borderRadius: theme.spacing.unit * 9,
    marginBottom: theme.spacing.unit,
    marginTop: theme.spacing.unit
  },
  search: {
    padding: '2px 4px',
    display: 'flex',
    alignItems: 'center',
    width: 'auto',
    boxShadow: 'none',
    border: '1px solid #ccc',
    marginTop: theme.spacing.unit,
    marginBottom: theme.spacing.unit
  },
  input: {
    marginLeft: theme.spacing.unit,
    flex: 1
  },
  iconButton: {
    padding: '5px 10px'
  }
});

export default sideBarStyles;
