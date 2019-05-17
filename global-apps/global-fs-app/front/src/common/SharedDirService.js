import path from 'path';

class SharedDirService {
  static URL = '/share';

  static share(dirName, participants) {
    let url = path.join(this.URL, dirName);
    console.log(url);

    return fetch(url, {
      method: 'POST',
      body: JSON.stringify(participants)
    });
  }

}

export default SharedDirService;
