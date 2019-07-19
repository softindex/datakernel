const breadcrumbsStyles = theme => ({
  root: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: theme.spacing.unit,
    minHeight: theme.spacing.unit * 6
  },
  icon: {
    marginRight: theme.spacing.unit,
    marginLeft: theme.spacing.unit
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
    marginLeft: theme.spacing.unit
  }
});

export default breadcrumbsStyles;
