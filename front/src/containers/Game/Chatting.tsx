"use client";

import { useEffect, useState } from "react";
import type { Frame } from "stompjs";
import { useWebSocket } from "@/socket/WebSocketProvider";
import styles from "./game.module.scss";

export default function Chatting() {
  type Message = {
    sender: string;
    msg: string;
    sendTime: string;
  };

  const [chatLog, setChatLog] = useState<Message[]>([]);
  const stompClient = useWebSocket();
  const [message, setMessage] = useState({
    sender: "",
    msg: "",
    sendTime: "",
  });

  // 메시지 핸들링 함수
  // const handleMessage = (frame: Frame) => {
  //   const receivedMessage = JSON.parse(frame.body);
  //   setChatLog((prevChatLog) => [...prevChatLog, receivedMessage]);
  // };

  useEffect(() => {
    if (stompClient) {
      // stompClient를 사용하여 채팅 메시지를 구독합니다.
      const subscription = stompClient.subscribe(
        "/sub/lobby/chat",
        function handleMessageFunction(frame: Frame) {
          const receivedMessage = JSON.parse(frame.body);
          setChatLog((prevChatLog) => [...prevChatLog, receivedMessage]);
        },
        {}
      );

      return () => {
        // 컴포넌트가 언마운트될 때 구독을 언서브스크라이브합니다.
        subscription.unsubscribe();
      };
    }
    return undefined;
  }, [stompClient]);

  // 메시지 전송 함수
  function sendMessage() {
    if (stompClient) {
      // const curr = new Date();
      // let hour = String(curr.getHours());
      // let minutes = String(curr.getMinutes());
      // let sec = String(curr.getSeconds());
      // if (hour.length === 1) {
      //   hour = `0${hour}`;
      // }
      // if (minutes.length === 1) {
      //   minutes = `0${minutes}`;
      // }
      // if (sec.length === 1) {
      //   sec = `0${sec}`;
      // }
      // const time = `${hour}:${minutes}:${sec}`;

      // message.sendTime = String(time);

      // 펴블리셔
      stompClient.send("/pub/lobby/chat", {}, JSON.stringify(message));

      // 입력 필드 초기화
      setMessage({
        sender: "",
        msg: "",
        sendTime: "",
      });
    }
  }

  // 받은 메시지 관리

  // Message 핸들러
  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { id, value } = e.target;
    setMessage((prevMessage) => ({
      ...prevMessage,
      [id]: value,
    }));
  };

  return (
    <>
      <div className={styles.chatLog}>
        {chatLog.map((messages, idx) => (
          <div key={messages.sendTime + String(idx)}>
            <strong>{messages.sender}: </strong>
            {messages.msg}
          </div>
        ))}
      </div>
      <div className={styles.chatInput}>
        <input
          type="text"
          id="sender"
          key="sender"
          value={message.sender}
          onChange={(e) => handleInputChange(e)}
          placeholder="보낸이"
        />
        <input
          type="text"
          key="msg"
          id="msg"
          value={message.msg}
          onChange={(e) => handleInputChange(e)}
          placeholder="메시지"
        />
        <button type="submit" onClick={sendMessage}>
          ㄱ
        </button>
      </div>
    </>
  );
}
