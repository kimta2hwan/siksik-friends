package com.ssf.socket.controller;

import com.ssf.socket.domain.History;
import com.ssf.socket.domain.HistoryMember;
import com.ssf.socket.domain.Member;
import com.ssf.socket.dto.HistoryDTO;
import com.ssf.socket.service.QuizSaveService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
public class HistoryController {
    private final MongoTemplate mongoTemplate;
    private final QuizSaveService quizSaveService;

    @Autowired
    public HistoryController(MongoTemplate mongoTemplate, QuizSaveService quizSaveService) {
        this.mongoTemplate = mongoTemplate;
        this.quizSaveService = quizSaveService;
    }

    @PostMapping("/api/socket/history")
    public HistoryDTO getHistory(@RequestBody Member user) {
        
        HistoryDTO allAboutUser = new HistoryDTO();
        
        // "member" 컬렉션에서 userId를 이용하여 historyList를 가져옵니다.
        Query memberQuery = new Query(Criteria.where("userId").is(user.getUserId()));

        HistoryMember member = mongoTemplate.findOne(memberQuery, HistoryMember.class);

        if (member == null) {
            // 사용자를 찾을 수 없을 경우 적절한 처리를 수행합니다.
            return null;
        }

        allAboutUser.setAllSolvedQuizCount(member.getAllSolvedQuizCount());
        allAboutUser.setAllCorrectQuizCount(member.getAllCorrectQuizCount());

        allAboutUser.setEconomySolvedQuizCount(member.getEconomySolvedQuizCount());
        allAboutUser.setEconomyCorrectQuizCount(member.getEconomyCorrectQuizCount());

        allAboutUser.setLivingSolvedQuizCount(member.getLivingSolvedQuizCount());
        allAboutUser.setLivingCorrectQuizCount(member.getLivingCorrectQuizCount());

        allAboutUser.setScienceSolvedQuizCount(member.getScienceSolvedQuizCount());
        allAboutUser.setScienceCorrectQuizCount(member.getScienceCorrectQuizCount());

        allAboutUser.setSocialSolvedQuizCount(member.getSocialSolvedQuizCount());
        allAboutUser.setSocialCorrectQuizCount(member.getSocialCorrectQuizCount());

        allAboutUser.setGlobalSolvedQuizCount(member.getGlobalSolvedQuizCount());
        allAboutUser.setGlobalCorrectQuizCount(member.getGlobalCorrectQuizCount());


        List<History> allHistory = new ArrayList<>();

        // historyList 내부의 historyId 값을 가져와서 "history" 컬렉션에서 정보를 가져옵니다.
        List<Long> historyList = member.getHistoryList();

        for (Long historyId : historyList) {
            Query historyQuery = new Query(Criteria.where("historyId").is(historyId));
            History history = mongoTemplate.findOne(historyQuery, History.class);

            allHistory.add(history);
        }

        allAboutUser.setAllHistory(allHistory);

        // roomId에 대한 정보를 찾을 수 없을 경우 적절한 처리를 수행합니다.
        return allAboutUser;
    }

    @DeleteMapping("/api/socket/delete")
    public String deleteHistory () {
        quizSaveService.deleteAllHistory();
        return "모든 전적 삭제";
    }
}

