import React, {Component} from 'react';
import {getRandomNumberInRange, getRandomArrayElement} from './utils';

let ctx;

class Node {
  constructor(x, y, nodesSize, size) {
    this.x = this._x = x;
    this.y = this._y = y;
    this._nodesSize = nodesSize;
    this.connections = [];
    this.r = getRandomNumberInRange(-size, size);
  }

  draw() {
    ctx.strokeStyle = '#212121';
    ctx.fillStyle = '#212121';
    ctx.lineWidth = 0.3;
    ctx.fillRect(this.x - this._nodesSize / 2, this.y - this._nodesSize / 2, this._nodesSize, this._nodesSize);

    for (let i = 0; i < this.connections.length; i++) {
      ctx.beginPath();
      ctx.moveTo(this.x, this.y);
      ctx.lineTo(this.connections[i].x, this.connections[i].y);
      ctx.stroke();
    }
  }
}

class SignUpAbstractionImage extends Component {
  componentDidMount() {
    this.renderAnimation();
  }

  renderAnimation() {
    const {nodesCount, nodesSize, size} = this.props;
    const viewWidth = window.innerHeight * 1.6;
    const viewHeight = window.innerHeight * 1.6;
    const drawingCanvas = this._canvas;

    initDrawingCanvas();
    const nodes = createNodes();
    connectNodes();
    draw();

    function initDrawingCanvas() {
      drawingCanvas.width = viewWidth;
      drawingCanvas.height = viewHeight;
      ctx = drawingCanvas.getContext('2d');
    }

    function createNodes() {
      const rad = viewWidth * 0.5 - 10;
      const nodes = [];

      for (let i = 0; i < nodesCount; i++) {
        const q = Math.random() * (Math.PI * 2);
        const r = Math.sqrt(Math.random());
        const x = (rad * r) * Math.cos(q) + viewWidth * 0.5;
        const y = (rad * r) * Math.sin(q) + viewWidth * 0.5;
        nodes[i] = new Node(x, y, ctx, nodesSize, size);
      }

      return nodes;
    }

    function connectNodes() {
      let connection;
      let j;
      let connectCount;

      for (let i = 0; i < nodes.length; i++) {
        j = 0;

        connectCount = Math.floor(getRandomNumberInRange(2, 3));

        while (j < connectCount) {
          connection = getRandomArrayElement(nodes);

          if (nodes[i] !== connection) {
            nodes[i].connections.push(connection);
            j++;
          }
        }
      }
    }

    function draw() {
      ctx.clearRect(0, 0, viewWidth, viewHeight);
      nodes.forEach((n) => {
        n.draw();
      });
    }
  }

  render() {
    return (
      <canvas className={this.props.className} ref={c => this._canvas = c}/>
    );
  }
}

export default SignUpAbstractionImage;

