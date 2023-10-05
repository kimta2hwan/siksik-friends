package com.ssf.socket.controller;

import com.ssf.socket.domain.Member;
import com.ssf.socket.domain.Quiz;
import com.ssf.socket.domain.Room;
import com.ssf.socket.dto.*;
import com.ssf.socket.repository.MemoryRoomRepository;
import com.ssf.socket.repository.RoomRepository;
import com.ssf.socket.service.QuizSaveService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@RestController
@RequiredArgsConstructor
public class StompGameController {
    private final QuizSaveService quizSaveService;
    private final SimpMessagingTemplate messageTemplate;
    private final RedisTemplate<String, String> redisTemplate;

    private final RoomRepository roomRepository = new MemoryRoomRepository();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();


    @MessageMapping("/game/start/{roomId}")
    public void gameStart(@DestinationVariable int roomId,
                          @Payload Room body) {

        String category = body.getCategory();

        String date = body.getQuizDate();
        Quiz quizList = quizSaveService.getQuiz(date, category);

        scheduler.schedule(() -> loading(roomId, body), 0, TimeUnit.SECONDS); // 0, 23, 46, 69, ...

        int time = 3;
        for (int i = 0; i < body.getQuizCount(); i++) {
            int idx = i;
            scheduler.schedule(() -> sendQuiz(roomId, quizList.getQuizSet().get(idx)), time, TimeUnit.SECONDS);
            time += 1;
            scheduler.schedule(() -> sendResult(roomId), time, TimeUnit.SECONDS);
            time += 1;
        }
        scheduler.schedule(() -> endGame(roomId, quizList.getQuizSet(), category, body.getQuizCount()), time, TimeUnit.SECONDS);
    }

    public void loading(int roomId, Room roomInfo) {

        String start = "start!";

        Room targetRoom = roomRepository.findByRoomId(roomId).orElseThrow();

        targetRoom.setRoomStatus(1);

        List<Room> rooms = roomRepository.findAll();

        messageTemplate.convertAndSend("/sub/game/quiz/" + roomId, start);
        messageTemplate.convertAndSend("/sub/room/roomList", rooms);
    }

    public void sendQuiz(int roomId, QuizDTO quiz) {

        messageTemplate.convertAndSend("/sub/game/quiz/" + roomId, quiz);
    }

    @PostMapping("/socket/score")
    @CrossOrigin(origins = "*")
    public List<Member> scoreCalculation(
            @RequestBody ReplyDTO replyDto) {
        if (replyDto.getAnswer().equals(replyDto.getUserAnswer())) {
            roomRepository.saveScore(replyDto.getRoomId(), replyDto.getUserId());
        }
        return roomRepository.findByRoomId(replyDto.getRoomId()).orElseThrow().getMembers();
    }

    public void sendResult(long roomId) {

        List<Member> result = roomRepository.findByRoomId(roomId).orElseThrow().getMembers();

        result.sort(Comparator.comparingInt(e -> -e.getGameScore()));

        messageTemplate.convertAndSend("/sub/game/result/" + roomId, result);
    }
    public void endGame(int roomId, List<QuizDTO> quizzes, String category, int allQuizCount) {

        String end = "end!";

        List<Member> members = roomRepository.findByRoomId(roomId).orElseThrow().getMembers();

        quizSaveService.pushMember(roomId, members, category, allQuizCount);

        for (Member member : members) {
            redisTemplate.opsForZSet().incrementScore("rank", member.getUserId().toString(), member.getGameScore());
        }


        for (QuizDTO quiz : quizzes) {

            quizSaveService.pushHistory(roomId, quiz);
        }


        messageTemplate.convertAndSend("/sub/game/end/" + roomId, end);
    }

}