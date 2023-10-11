package com.ssf.auth.global.jwt.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.ssf.auth.domain.user.dto.UserRequest;
import com.ssf.auth.domain.user.repository.UserRepository;
import com.ssf.auth.global.jwt.dto.JwtDto;
import jakarta.servlet.http.HttpServletResponse;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static com.ssf.auth.global.jwt.domain.JwtConstants.*;


@Service
@Slf4j
@Transactional
@Getter
@RequiredArgsConstructor
public class JwtService {

    @Value("${jwt.secretKey}")
    private String secretKey;

    @Value("${jwt.access.expiration}")
    private Long accessTokenExpirationPeriod;

    @Value("${jwt.refresh.expiration}")
    private Long refreshTokenExpirationPeriod;

    @Value("${jwt.access.header}")
    private String accessHeader;

    @Value("${jwt.refresh.header}")
    private String refreshHeader;

    private final RedisTemplate<String, String> redisTemplate;
    private final UserRepository userRepository;

    public String createAccessToken(String id) {
        Date now = new Date();

        return JWT.create()
                .withSubject(ACCESS_TOKEN_SUBJECT.getValue())
                .withExpiresAt(new Date(now.getTime() + accessTokenExpirationPeriod))
                .withClaim(ID_CLAIM.getValue(), Long.parseLong(id))
                .sign(Algorithm.HMAC512(secretKey));
    }

    public String createRefreshToken() {
        Date now = new Date();

        return JWT.create()
                .withSubject(REFRESH_TOKEN_SUBJECT.getValue())
                .withExpiresAt(new Date(now.getTime() + refreshTokenExpirationPeriod))
                .sign(Algorithm.HMAC512(secretKey));
    }

    public void sendAccessToken(HttpServletResponse response, String accessToken) {
        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader(accessHeader, accessToken);
    }

    public JwtDto extractHeader(UserRequest.AccessHeader accessHeader) {
        String accessToken = extractAccessToken(accessHeader);
        DecodedJWT decodedJWT = extractJwt(accessToken);

        return JwtDto.builder()
                .accessToken(accessToken)
                .id(extractId(decodedJWT))
                .exp(extractExpiration(decodedJWT))
                .build();
    }

    public String extractAccessToken(UserRequest.AccessHeader accessHeader) {
        return accessHeader.accessHeader().replace(AUTH_TYPE.getValue(), "");
    }

    private DecodedJWT extractJwt(String accessToken) {
        return JWT.require(Algorithm.HMAC512(secretKey))
                    .build()
                    .verify(accessToken);
    }

    public String extractId(DecodedJWT decodedJWT) {
        return String.valueOf(decodedJWT.getClaim(ID_CLAIM.getValue()));
    }

    public Long extractExpiration(DecodedJWT decodedJWT) {
        return decodedJWT.getClaim(EXP_CLAIM.getValue()).asLong() * 1000
                - System.currentTimeMillis();
    }

    public String reIssuanceAccessToken(UserRequest.AccessHeader accessHeader) {
        return createAccessToken(extractHeader(accessHeader).id());
    }

    public void updateRefreshToken(String id, String refreshToken) {
        redisTemplate.opsForValue().set(id, refreshToken, refreshTokenExpirationPeriod, TimeUnit.MILLISECONDS);
    }
}
