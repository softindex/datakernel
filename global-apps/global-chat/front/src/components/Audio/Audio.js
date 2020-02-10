import React, {useEffect, useRef} from 'react';

function Audio({src}) {
  const ref = useRef(null);

  useEffect(() => {
    ref.current.srcObject = src;
  }, [src]);

  return (
    <audio ref={ref} controls volume="true" autoPlay style={{display: 'none'}}/>
  );
}

export default Audio;
