import { createMuiTheme } from '@material-ui/core/styles';

const theme = createMuiTheme({
  palette: {
    primary: {
      main: '#3e79ff'
    },
    secondary: {
      main: '#2196F3'
    },
    action: {
      selected: 'rgba(255, 87, 34, 0.15)'
    },
  }
});

export default theme;
