package com.spring.jpa.domain.User;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UserCreateForm {
    private String username;
    private String password1;
    private String password2;
    private String email;
}
