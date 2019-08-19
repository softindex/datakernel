
window.onload = () => document.getElementById('withMetadata').addEventListener('change', handleWithMetadata);

function handleWithMetadata(e){
  if (e.target.checked){
  const form = document.getElementById('form');
  const metadataDiv = document.createElement('div');
  metadataDiv.id='metadata';
  metadataDiv.innerHTML=` <label for="title">Title: </label>
                             <input type="text" name="title" id="title" required><br>
                             <label for="description">Description: </label>
                             <textarea name="description" id="description" placeholder="Enter description..."></textarea><br>
                             `
  form.insertBefore(metadataDiv, document.getElementById('submit'));
  } else {
    document.getElementById('metadata').remove();
  }
}
