package com.spring.jpa.global.oauth;

import com.spring.jpa.domain.member.OAuthProvider;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;


public class KakaoLoginParams implements OAuthLoginParams{
    private String authorizationCode;

    @Override
    public OAuthProvider oAuthProvider(){
        return OAuthProvider.KAKAO;
    }

    @Override
    public MultiValueMap<String, String> makeBody() {
        MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
        body.add("code", authorizationCode);
        return body;
    }

}
