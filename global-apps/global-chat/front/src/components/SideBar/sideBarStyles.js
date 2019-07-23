const sideBarStyles = theme => {
  return {
    wrapper: {
      boxShadow: `2px 0px ${theme.spacing.unit}px -2px rgba(0,0,0,0.2)`,
      width: 350,
      height: '100vh',
      display: 'flex',
      flexDirection: 'column',
      flexGrow: 0,
      flexShrink: 0,
      position: 'static'
    },
    paper: {
      boxShadow: `0px ${theme.spacing.unit}px  ${theme.spacing.unit}px -5px rgba(0,0,0,0.2)`,
      background: theme.palette.primary.background
    },
    chatsList: {
      flexGrow: 1,
      overflowY: 'auto',
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
      width: theme.spacing.unit*41,
      borderRadius: 70,
      marginBottom: theme.spacing.unit,
      marginTop: theme.spacing.unit
    },
    search: {
      padding: `${theme.spacing.unit}px 0px`,
      display: 'flex',
      alignItems: 'center',
      boxShadow: 'none',
      border: 'none',
      flexGrow: 0,
      paddingBottom: theme.spacing.unit,
      marginTop: theme.spacing.unit * 8
    },
    inputDiv: {
      marginLeft: theme.spacing.unit,
      marginRight: theme.spacing.unit * 3,
      flex: 1
    },
    input: {
      textOverflow: 'ellipsis',
      overflow: 'hidden',
      whiteSpace: 'nowrap'
    },
    iconButton: {
      padding: `${theme.spacing.unit}px ${theme.spacing.unit}px`
    }
  }
};

export default sideBarStyles;