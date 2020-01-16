import {useMemo} from 'react';
import {useSnackbar as useNotistackSnackbar} from 'notistack';

const LOADING_SNACKBAR_DELAY = 500;

export function useSnackbar() {
  const {enqueueSnackbar, closeSnackbar} = useNotistackSnackbar();
  const key = useMemo(() => Math.random(), []);
  const state = useMemo(() => ({}), []);

  return {
    showSnackbar(message, type) {
      switch (type) {
        case 'error':
          enqueueSnackbar(message, {
            variant: 'error',
            key
          });
          break;
        case 'loading':
          state.timeoutId = setTimeout(() => {
            enqueueSnackbar(message, {
              key,
              persist: true
            });
          }, LOADING_SNACKBAR_DELAY);
          break;
        default: break;
      }
    },
    hideSnackbar() {
      clearTimeout(state.timeoutId);
      closeSnackbar(key);
    }
  };
}
