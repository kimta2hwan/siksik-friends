package com.ssf.member.domain.user.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;

@Getter
@NoArgsConstructor
public class UserDto {

    @Getter
    @Builder
    @AllArgsConstructor
    public static class Detail {

        private String email;
        private String nickname;
        private String profile;
        private int level;
        private String odds;
    }

    @Getter
    @Builder
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Request {

        private Long user_id;
        private String nickname;
        private String password;
        private String changePassword;
        private String profile;

        public void encodePassword(PasswordEncoder passwordEncoder) {
            this.changePassword = passwordEncoder.encode(this.changePassword);
        }
    }

    @Getter
    @Builder
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Response implements Comparable<Response> {

        private Long user_id;
        private Long size;

        private String email;
        private String nickname;
        private String profile;
        private String odds;

        private Boolean activated;
        private Long rank;
        private Long exp;
        private Integer score;
        private Integer level;

        @Override
        public int compareTo(Response response) {
            if (this.activated == response.activated) {
                return this.nickname.toLowerCase().compareTo(response.nickname.toLowerCase());
            }

            if (this.activated) {
                return 1;
            }

            return 0;
        }
    }
}
