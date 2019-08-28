import {useEffect, useState} from 'react';

export function useService(service) {
  const [state, setState] = useState(service.getAll());
  useEffect(() => {
    service.addChangeListener(setState);
    return () => {
      service.removeChangeListener(setState);
    };
  }, [service]);

  return state;
}
