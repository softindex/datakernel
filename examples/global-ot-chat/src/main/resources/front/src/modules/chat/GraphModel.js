class GraphModel {
  async getGraph(revision) {
    const response = await fetch(`/graph?id="${encodeURIComponent(revision)}"`);
    return await response.text();
  }
}

export default GraphModel;
