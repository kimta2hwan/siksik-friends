package com.ssf.socket.domain;

import com.ssf.socket.dto.QuizDTO;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class Quiz {
    String quizId;
    String quizDate;
    List<QuizDTO> quizSet;
}