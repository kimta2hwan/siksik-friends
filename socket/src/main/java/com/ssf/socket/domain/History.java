package com.ssf.socket.domain;

import com.ssf.socket.dto.ArticleDTO;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@Document(collection = "history")
public class History {

    @Id
    int historyId;
    String category;
    String solvedDate;
    String articlesDate;
    String roomName;
    List<ArticleDTO> articles;

}
