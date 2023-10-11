package com.ssf.socket.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
public class ReplyDTO {
    long roomId;
    Long userId;
    String userAnswer;
    int userScore;
    int quizNumber;
    String quizType;
    String question;
    String hint;
    String answer;
    String articleTitle;
    String articleContent;
}
