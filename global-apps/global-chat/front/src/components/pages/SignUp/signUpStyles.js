const signUpStyles = theme => {
  return {
    root: {
      height: '100vh'
    },
    buttonWrapper: {
      display: 'flex',
      flexDirection: 'row',
      justifyContent: 'flex-start',
      alignItems: 'center',
      flexWrap: 'wrap'
    },
    button: {
      minHeight: theme.spacing.unit * 7.5,
      borderRadius: 70
    },
    signupButton: {
      '&:hover': {
        backgroundColor: theme.palette.primary.light
      }
    },
    caption: {
      marginTop: theme.spacing.unit * 3
    },
    attachIcon: {
      transform: 'rotate(45deg)',
      marginRight: theme.spacing.unit
    },
    storeIcon: {
      marginRight: theme.spacing.unit
    },
    description: {
      marginBottom: '100px',
      [theme.breakpoints.down('sm')]: {
        fontSize: '1rem'
      }
    },
    container: {
      height: '100%',
      display: 'flex',
      flexDirection: 'row'
    },
    column: {
      position: 'relative',
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'center',
      alignItems: 'flex-start',
      zIndex: 1,
      [theme.breakpoints.only('xs')]: {
        padding: theme.spacing.unit * 2
      },
      [theme.breakpoints.only('sm')]: {
        padding: theme.spacing.unit * 6
      },
      [theme.breakpoints.only('md')]: {
        padding: theme.spacing.unit * 10
      },
      [theme.breakpoints.up('lg')]: {
        padding: theme.spacing.unit * 16
      }
    },
    columnRight: {
      overflow: 'hidden',
      position: 'relative',
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'center',
      alignItems: 'flex-start',
      zIndex: 1
    },
    animation: {
      position: 'absolute',
      right: '-50%',
      bottom: '-30%',
      zIndex: 1
    },
    gradientOverlay: {
      position: 'absolute',
      top: 0,
      left: 0,
      width: '100%',
      height: '100%',
      backgroundImage: `linear-gradient(120deg, ${theme.palette.background.default} 50%, rgba(235, 237, 238, 0) 100%)`,
      backgroundPosition: '-100% 0%',
      backgroundSize: '200% 100%',
      zIndex: 2
    },
    title: {
      [theme.breakpoints.down('sm')]: {
        fontSize: '3.2rem'
      }
    },
    input: {
      display: 'none'
    }
  }
};

export default signUpStyles;
