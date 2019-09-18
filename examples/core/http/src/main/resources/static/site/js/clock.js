function updateClock() {
    const now = new Date();
    const time = now.getHours() + ':' + now.getMinutes() + ':' + now.getSeconds();
    console.log(time);
    document.getElementById('time').innerHTML = time;

    setTimeout(updateClock, 1000);
}
