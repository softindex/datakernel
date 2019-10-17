const breadcrumbsStyles = theme => ({
  root: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: theme.spacing(1),
    minHeight: theme.spacing(6)
  },
  icon: {
    marginRight: theme.spacing(1),
    marginLeft: theme.spacing(1)
  },
  button: {
    textTransform: 'capitalize'
  },
  breadcrumbItem: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center'
  },
  grow: {
    flexGrow: 1
  },
  dropDownIcon: {
    marginLeft: theme.spacing(1)
  }
});

export default breadcrumbsStyles;
