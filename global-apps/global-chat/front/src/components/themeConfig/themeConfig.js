import { createMuiTheme } from '@material-ui/core/styles';

const theme = createMuiTheme({
  palette: {
    primary: {
      main: '#3e79ff',
      background: '#f8f8f8',
      contrastText: '#fff',
      darkWhite: '#f5f5dc'
    },
    secondary: {
      main: '#f44336',
      contrastText: '#000',
      darkGrey: '#808080',
      grey: '#66666680'
    }
  }
});

export default theme;
