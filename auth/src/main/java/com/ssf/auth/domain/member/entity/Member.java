package com.ssf.auth.domain.member.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Entity
@Table
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MemberEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 320)
    private String email;

    @Column(nullable = false, length = 255)
    private String password;

    @Column(nullable = false, length = 16)
    private String nickname;

    @Column(nullable = false)
    @ColumnDefault("1000")
    private int score;

    @Column(nullable = false, length = 2083)
    @ColumnDefault("/profile.png")
    private String profile;

    @UpdateTimestamp
    @Column(length = 20)
    private LocalDateTime signUpAt;
}
