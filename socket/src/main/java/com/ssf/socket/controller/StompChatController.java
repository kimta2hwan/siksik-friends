package com.ssf.socket.controller;

import com.ssf.socket.dto.ChatMessageDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.Date;


@Slf4j
@RestController
@RequiredArgsConstructor
public class StompChatController {

    private final SimpMessagingTemplate messageTemplate; //특정 Broker로 메세지를 전달

    @MessageMapping("/lobby/chat")
    public void sendMsgToLobby(
            @Payload ChatMessageDTO body) {

        Date currentTime = new Date();

        SimpleDateFormat sdf = new SimpleDateFormat("hh:mm a");
        String formattedTime = sdf.format(currentTime);

        body.setSendTime(formattedTime);

        messageTemplate.convertAndSend("/sub/lobby/chat", body);
    }
    @MessageMapping("/room/chat/{roomId}")
    public void sendMsgToRoom(
            @DestinationVariable int roomId,
            @Payload ChatMessageDTO body) {

        Date currentTime = new Date();

        SimpleDateFormat sdf = new SimpleDateFormat("hh:mm a");
        String formattedTime = sdf.format(currentTime);

        body.setSendTime(formattedTime);

        messageTemplate.convertAndSend("/sub/room/chat/" + roomId, body);
    }
}