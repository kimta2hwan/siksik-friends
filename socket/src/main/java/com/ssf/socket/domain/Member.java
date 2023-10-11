package com.ssf.socket.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class Member {

    private Long userId;
    private String userName;
    private int gameScore;
    private int userScore;
    private int userRanking;

    private int gameQuizCount;
    private int gameCorrect;

    private String gameType;

    private int level;
    private String profile;

    // True == 방장, False == 일반
    private boolean isLeader;

    // True == 레디, False == 언레디
    private boolean isReady;
}
