package com.ssf.socket.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class ArticleDTO {

    String articleTitle;
    String articleAnswer;
    List<String> articleQuiz;

}
