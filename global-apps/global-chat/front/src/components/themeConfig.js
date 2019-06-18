import { createMuiTheme } from '@material-ui/core/styles';

const theme = createMuiTheme({
  palette: {
    primary: {
      main: '#3e79ff',
      contrastText: '#fff'
    },
    action: {
      selected: 'rgba(255, 87, 34, 0.15)'
    },
    secondary: {
      main: '#f44336',
      contrastText: '#000'
    }
  }
});

export default theme;
