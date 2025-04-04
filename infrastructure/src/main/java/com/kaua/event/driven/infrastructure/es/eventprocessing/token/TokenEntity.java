package com.kaua.event.driven.infrastructure.es.eventprocessing.token;

import jakarta.persistence.*;

@Entity
@Table(name = "token")
public class TokenEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String processorName;

    private String tokenClass;

    @Column(name = "token", length = 10000)
    private String token;

    public TokenEntity() {
    }

    public TokenEntity(String processorName, String tokenClass, String token) {
        this.processorName = processorName;
        this.tokenClass = tokenClass;
        this.token = token;
    }

    public Long getId() {
        return id;
    }

    public String getProcessorName() {
        return processorName;
    }

    public String getTokenClass() {
        return tokenClass;
    }

    public void setTokenClass(String tokenClass) {
        this.tokenClass = tokenClass;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public void setProcessorName(String processorName) {
        this.processorName = processorName;
    }
}
