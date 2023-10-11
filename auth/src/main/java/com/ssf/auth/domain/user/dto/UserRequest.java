package com.ssf.auth.domain.user.dto;

import com.ssf.auth.global.common.ValidationGroups;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.Builder;
import lombok.Getter;

public record UserRequest() {

    private static final String NOT_BLANK_EMAIL = "이메일은 필수 입력 값입니다.";
    private static final String IMPOSSIBLE_EMAIL = "이메일 형식이 올바르지 않습니다.";
    private static final String NOT_BLANK_NICKNAME = "닉네임은 필수 입력 값입니다.";
    private static final String IMPOSSIBLE_NICKNAME = "닉네임 형식이 올바르지 않습니다.";
    private static final String EMAIL_REGEXP = "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+.[A-Za-z]{2,6}$";
    private static final String NICKNAME_REGEXP = "^[가-힣a-zA-Z0-9._ -]{2,}$";

    @Builder
    public record Email(
            @NotBlank(message = NOT_BLANK_EMAIL, groups = ValidationGroups.NotNullGroup.class)
            @Pattern(
                    regexp = EMAIL_REGEXP,
                    message = IMPOSSIBLE_EMAIL,
                    groups = ValidationGroups.PatternCheckGroup.class
            )
            String email
    ) {}

    @Builder
    public record Nickname(
            @NotBlank(message = NOT_BLANK_NICKNAME, groups = ValidationGroups.NotNullGroup.class)
            @Pattern(
                    regexp = NICKNAME_REGEXP,
                    message = IMPOSSIBLE_NICKNAME,
                    groups = ValidationGroups.PatternCheckGroup.class
            )
            String nickname
    ) {}

    @Getter
    @Builder
    public static class SignUp {

        @NotBlank(message = NOT_BLANK_EMAIL, groups = ValidationGroups.NotNullGroup.class)
        @Pattern(regexp = EMAIL_REGEXP, message = IMPOSSIBLE_EMAIL, groups = ValidationGroups.PatternCheckGroup.class)
        private final String email;

        private final String password;

        @NotBlank(message = NOT_BLANK_NICKNAME, groups = ValidationGroups.NotNullGroup.class)
        @Pattern(
                regexp = NICKNAME_REGEXP,
                message = IMPOSSIBLE_NICKNAME,
                groups = ValidationGroups.PatternCheckGroup.class
        )
        private final String nickname;

        private String profile;

        public void changeProfile(String profile) {
            this.profile = profile;
        }
    }

    @Builder
    public record AccessHeader(String accessHeader) {}
}
