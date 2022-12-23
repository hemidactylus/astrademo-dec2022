import './App.css';
import axios from "axios";

import { useEffect, useState } from "react"

const uuid = require('uuid');
const userId = uuid.v4();

let ws = null;

function sendMessage(room, sender, message, setMessages) {
  axios.post(
    `http://localhost:8000/post/${room}`,
    {
      message,
      sender,
    }
  ).then(resp => {
    console.log(resp.data);
    refreshData(room, setMessages);
  });
}

function refreshData(room, setMessages){
  axios.get(`http://localhost:8000/post/${room}`).then( resp => {
    console.log(resp.data);
    setMessages(resp.data);
  });
}

function App() {

  const [roomName, setRoomName] = useState('food');
  const [userName, setUserName] = useState('john');
  const [msgText, setMsgText] = useState('');
  const [messages, setMessages] = useState([]);

  useEffect(
    () => {
      if ( ws === null ) {
        console.log('should ws');
        ws = new WebSocket(`ws://localhost:8001/updates/${userId}`);
        ws.onmessage = ev => {
          const newMsg = JSON.parse(ev.data);
          console.log(`RECEIVED ${JSON.stringify(newMsg)}`);
          // no duplicates
          if(newMsg.room === roomName){
            setMessages(ms => {
              console.log(`UNIQ ${ms.map( m => m.id)} HAS? ${newMsg.id}`);
              console.log(`UNIQ2 ${ms.map( m => m.id).indexOf(newMsg.id)}`);
              if (ms.map( m => m.id).indexOf(newMsg.id) === -1){
                return ([newMsg]).concat(ms)
              } else {
                return ms;
              }
            })
          }
        }
      }
    },
    []
  );

  console.log(JSON.stringify(messages));

  return (
    <div className="App">
      <header className="App-header">
        {userId}<br />
        Room: <input
          type="text"
          name="room"
          value={roomName}
          onChange={(e) => setRoomName(e.target.value)}
        />
        User:
        <input
          type="text"
          name="user"
          value={userName}
          onChange={(e) => setUserName(e.target.value)}
        />
        Message:
        <input
          type="text"
          name="message"
          value={msgText}
          onChange={(e) => setMsgText(e.target.value)}
          onKeyPress={(e) => {if (e.key === 'Enter') {sendMessage(roomName, userName, msgText, setMessages); setMsgText('')}}}
        />
        <button
          onClick={(e) => refreshData(roomName, setMessages)}
        >
          Refresh
        </button>
        <hr width="100%" />
        <ul>
          {messages.map( msg => 
            <li key={msg.id}>{msg.sender}: {msg.message} [{msg.when}]</li>
          )}
        </ul>
      </header>
    </div>
  );
}

export default App;
