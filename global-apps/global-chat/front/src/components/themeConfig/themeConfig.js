import { createMuiTheme } from '@material-ui/core/styles';

const theme = createMuiTheme({
  palette: {
    primary: {
      main: '#3e79ff',
      background: '#f8f8f8',
      contrastText: '#fff',
      darkWhite: '#F5F5DC'
    },
    secondary: {
      main: '#f44336',
      background: '#e3f1fa',
      contrastText: '#000',
      grey: '#66666680',
      lightGrey: '#ccc',
      darkGrey: '#808080'
    }
  }
});

export default theme;
