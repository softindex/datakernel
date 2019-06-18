const sideBarStyles = (theme) => {
  return {
    wrapper: {
      //border: '2px solid black',
      borderRight: '2px solid #DCDCDC',
      marginTop: 66,
      width: '355px',
      flexGrow: 0,
      flexShrink: 0,
      position: 'static'
    },
    tabContent: {
      //border: '2px solid red',
      height: 'calc(100% - 115px)',
      position: 'fixed'
    },
    chatsList: {
      //border: '2px solid blue',
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
      marginBottom: 5,
      marginTop: 5
    }
  }
};

export default sideBarStyles;