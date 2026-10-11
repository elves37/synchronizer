package com.hi.usersync.api;

import com.hi.usersync.target.TargetUser;
import com.hi.usersync.target.TargetUserRepository;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/target-users")
public class TargetUserController {
    private final TargetUserRepository repository;

    public TargetUserController(TargetUserRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public List<TargetUser> search(@RequestParam(required = false) String keyword,
                                   @RequestParam(required = false) String syncMode,
                                   @RequestParam(defaultValue = "100") int limit) {
        return repository.search(keyword, syncMode, limit);
    }

    @GetMapping("/{accountKey}")
    public TargetUser get(@PathVariable String accountKey) {
        return repository.findByAccountKey(accountKey)
                .orElseThrow(() -> new IllegalArgumentException("Unknown account key: " + accountKey));
    }
}
