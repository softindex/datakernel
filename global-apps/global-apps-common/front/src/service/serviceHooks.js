import {useEffect, useState, useRef} from 'react';

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

export function initService(service, errorHandler) {
  useEffect(() => {
    service.init().catch(errorHandler);
    if (typeof service.stop === 'function') {
      return () => service.stop();
    }
  }, [service]);
}
