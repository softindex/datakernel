const sideBarStyles = (theme) => {
  return {
    sideBar: {
      backgroundColor: '#D3D3D3'
    },
    root: {
      display: 'flex'
    },
    drawer: {
      width: 1,
      flexShrink: 0
    },
    drawerPaper: {
      width: '355px'
    },
    toolbar: theme.mixins.toolbar
  }
};

export default sideBarStyles;