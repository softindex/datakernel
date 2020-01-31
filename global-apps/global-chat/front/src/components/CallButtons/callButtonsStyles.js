const callButtonsStyles = theme => ({
  fab: {
    margin: theme.spacing(1),
  },
  fabAnimation: {
    animationName: '$fab',
    animationDuration: '0.5s',
    animationIterationCount: 'infinite',
    animationDirection: 'alternate'
  },
  '@keyframes fab': {
    from: {
      boxShadow: `0 0 5px ${theme.palette.primary.main}`
    },
    to: {
      boxShadow: `0 0 20px ${theme.palette.primary.main}`
    }
  }
});

export default callButtonsStyles;
