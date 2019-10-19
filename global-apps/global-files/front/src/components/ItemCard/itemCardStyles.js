const itemCardStyles = theme => ({
  root: {
    width: theme.spacing(25),
    marginRight: theme.spacing(2),
    marginBottom: theme.spacing(2),
    [theme.breakpoints.down('xs')]: {
      width: `calc(50% - ${theme.spacing(0.5)}px)`,
      marginRight: theme.spacing(1),
      marginBottom: theme.spacing(1),
      '&:nth-child(even)': {
        marginRight: 0
      }
    }
  },
  headerItem: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    width: '100%',
    height: theme.spacing(20),
    backgroundColor: theme.palette.grey[200]
  },
  fileIcon: {
    fontSize: 80,
    color: theme.palette.grey[800]
  },
  folderIcon: {
    fontSize: 40,
    color: theme.palette.grey[800],
    marginRight: theme.spacing(2)
  },
  folderGroup: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center',
    padding: `${theme.spacing(1)}px ${theme.spacing(2)}px`
  },
  foldersLink: {
    textDecoration: 'none',
    color: theme.palette.secondary.contrastText
  }
});

export default itemCardStyles;
