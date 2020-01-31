import {useEffect, useState, useRef, useMemo} from 'react';

export function useService(service) {
  const prevService = useRef(service);
  const [state, setState] = useState(service.getAll());

  useEffect(() => {
    if (prevService.current !== service) {
      setState(service.getAll());
    }
    prevService.current = service;

    service.addChangeListener(setState);

    return () => {
      service.removeChangeListener(setState);
    };
  }, [service]);

  return state;
}

export function initService(service, errorHandler, needInitialize = true) {
  const state = useMemo(() => {
    return {initialized: false}
  }, []);

  useEffect(() => {
    state.initialized = false;
  }, [service]);

  useEffect(() => {
    if (needInitialize && typeof service.init === 'function') {
      state.initialized = true;
      service.init().catch(errorHandler);
    }

    if (typeof service.stop === 'function') {
      return () => {
        if (state.initialized) {
          service.stop();
        }
      };
    }
  }, [service, needInitialize]);
}
