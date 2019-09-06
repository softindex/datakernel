class GraphModel {
  static async getGraph(noteId) {
    const response = await fetch(`/ot/graph/${noteId}`);
    return await response.text();
  }
}

export default GraphModel;
