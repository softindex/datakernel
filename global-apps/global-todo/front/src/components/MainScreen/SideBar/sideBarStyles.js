const sideBarStyles = theme => {
  return {
    wrapper: {
      borderRight: '2px solid #DCDCDC',
      marginTop: theme.spacing.unit * 8,
      width: 355,
      flexGrow: 0,
      flexShrink: 0,
      position: 'static'
    },
    tabContent: {
      height: 'calc(100% - 115px)',
      position: 'fixed'
    },
    listsList: {
      height: 'calc(100% - 240px)',
      position: 'fixed',
      overflowY: 'auto',
      '&::-webkit-scrollbar-track': {
        background: 'border-box'
      },
      '&::-webkit-scrollbar-thumb': {
        background: '#66666680'
      }
    },
    button: {
      width: theme.spacing.unit*41,
      borderRadius: 70,
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
      marginLeft: 8,
      flex: 1
    },
    iconButton: {
      padding: '5px 10px'
    }
  }
};

export default sideBarStyles;
