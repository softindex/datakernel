import React, {useMemo, useContext, useEffect} from "react";

const globalContainer = new Map();

export function RegisterDependency({name, value, children, container = globalContainer}) {
  const Context = useMemo(() => {
    const Context = React.createContext(value);
    container.set(name, Context);
    return Context;
  }, [name]);

  useEffect(() => {
    return () => container.delete(name);
  }, [name]);

  return (
    <Context.Provider value={value}>
      {children}
    </Context.Provider>
  );
}

export function getInstance(name, container = globalContainer) {
  return useContext(container.get(name));
}
