package com.ssf.member.domain.user.service;

import com.ssf.member.domain.user.domain.User;
import com.ssf.member.domain.user.dto.UserDto;
import com.ssf.member.domain.user.dto.UserRequest;
import com.ssf.member.domain.user.dto.UserResponse;
import com.ssf.member.domain.user.repository.UserRepository;
import com.ssf.member.global.jwt.dto.JwtDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
@Transactional
@RequiredArgsConstructor
public class UserFindServiceImpl implements UserFindService {

    private static final String KEY = "rank";

    private final RedisTemplate<String, String> redisTemplate;
    private final UserRepository userRepository;

    @Override
    public UserResponse.MyInfo findUser(JwtDto jwtDto) {
        User user = userRepository.findById(Long.parseLong(jwtDto.id()))
                .orElseThrow();

        return UserResponse.MyInfo.builder()
                .user_id(user.getId())
                .email(user.getEmail())
                .nickname(user.getNickname())
                .profile(user.getProfile())
                .rank(findRank(user.getId()))
                .level(getLevel(user.getId()))
                .exp(user.getExp())
                .odds(user.getTotalGame() == 0L ? (user.getWin() == 0L ? "0.0%" : "100.0%")
                        : String.format("%.1f%%", user.getWin() / (double) user.getTotalGame() * 100))
                .build();
    }

    @Override
    public UserResponse.UserDetail findUserById(UserRequest.UserId userId) {
        log.info("service 시작");
        User user = userRepository.findById(userId.id()).orElseThrow(null);
        log.info("db user 생성");

        return UserResponse.UserDetail.builder()
                .user_id(user.getId())
                .nickname(user.getNickname())
                .profile(user.getProfile())
                .rank(findRank(userId.id()))
                .level(getLevel(user.getId()))
                .odds(user.getTotalGame() == 0L ? (user.getWin() == 0L ? "0.0%" : "100.0%")
                        : String.format("%.1f%%", user.getWin() / (double) user.getTotalGame() * 100))
                .build();
    }

    @Override
    public UserDto.Response findNickname(String nickname) {
        User user = userRepository.findByNickname(nickname).orElseThrow();

        return UserDto.Response.builder()
                .user_id(user.getId())
                .nickname(user.getNickname())
                .profile(user.getProfile())
                .rank(findRank(user.getId()))
                .level(getLevel(user.getId()))
                .odds(user.getTotalGame() == 0L ? (user.getWin() == 0L ? "0.0%" : "100.0%") : String.format("%.1f%%", user.getWin() / (double) user.getTotalGame() * 100))
                .build();
    }

    @Override
    public Long findRank(Long id) {
        Double ranking1 = redisTemplate.opsForZSet().score(KEY, String.valueOf(id));

        Set<String> ranking2 = redisTemplate.opsForZSet().reverseRangeByScore(KEY, ranking1, ranking1, 0, 1);
        Long ranking = 0L;

        for (String s : ranking2) {
            ranking = redisTemplate.opsForZSet().reverseRank(KEY, s);
        }

        return ranking + 1;
    }

    @Override
    public List<UserDto.Response> findRankList() {
        ZSetOperations<String, String> stringStringZSetOperations = redisTemplate.opsForZSet();
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringStringZSetOperations.reverseRangeWithScores(KEY, 0, 9);
        List<UserDto.Response> rankList = new ArrayList<>();
        AtomicReference<Long> i = new AtomicReference<>(1L);

        typedTuples.forEach(typedTuple -> {
            User user = userRepository.findById(Long.valueOf(typedTuple.getValue())).orElseThrow();
            rankList.add(UserDto.Response
                    .builder()
                    .user_id(user.getId())
                    .nickname(user.getNickname())
                    .profile(user.getProfile())
                    .level(getLevel(user.getId()))
                    .rank(i.getAndSet(i.get() + 1))
                    .score(typedTuple.getScore().intValue())
                    .odds(user.getTotalGame() == 0L ? (user.getWin() == 0L ? "0.0%" : "100.0%") : String.format("%.1f%%", user.getWin() / (double) user.getTotalGame() * 100))
                    .build());
        });

        return rankList;
    }

    @Override
    public int getLevel(Long id) {
        return (int) (redisTemplate.opsForZSet().score(KEY, String.valueOf(id)) / 1000);
    }
}
