async function request(resource, init) {
  const response = await fetch(resource, init);

  if (!response.ok) {
    let responseJson;
    try {
      responseJson = await resource.json();
    } catch (err) {
      throw new Error(response.statusText);
    }

    if (typeof responseJson.message === 'string') {
      throw new Error(responseJson.message);
    }

    throw new Error(response.statusText);
  }

  return response;
}

export default request;
