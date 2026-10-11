package com.hi.usersync.sync;

import org.springframework.stereotype.Component;

@Component
public class PasswordResolver {
    public String resolve(String secretKey) {
        // 운영에서는 Vault, AWS Secrets Manager, KMS, 사내 암호화 저장소 등으로 교체하세요.
        return secretKey;
    }
}
