const sideBarStyles = theme => {
  return {
    wrapper: {
      borderRight: '2px solid #DCDCDC',
      marginTop: `${theme.spacing.unit * 8}px`,
      width: 355,
      flexGrow: 0,
      flexShrink: 0,
      position: 'static'
    },
    tabContent: {
      height: 'calc(100% - 115px)',
      position: 'fixed'
    },
    chatsList: {
      height: 'calc(100% - 175px)',
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
      width: `${theme.spacing.unit*41}px`,
      borderRadius: 70,
      marginBottom: `${theme.spacing.unit}px`,
      marginTop: `${theme.spacing.unit}px`
    }
  }
};

export default sideBarStyles;