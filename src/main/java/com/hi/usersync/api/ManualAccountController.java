package com.hi.usersync.api;

import com.hi.usersync.manual.ManualAccountService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/manual-accounts")
public class ManualAccountController {
    private final ManualAccountService service;

    public ManualAccountController(ManualAccountService service) {
        this.service = service;
    }

    @PostMapping("/{accountKey}/exclude")
    public Map<String, Object> exclude(@PathVariable String accountKey,
                                       @Valid @RequestBody ExcludeRequest request) {
        service.exclude(accountKey, request.reason(), request.userId());
        return Map.of("accountKey", accountKey, "status", "MANUAL");
    }

    @PutMapping("/{accountKey}")
    public Map<String, Object> update(@PathVariable String accountKey,
                                      @Valid @RequestBody ManualUpdateRequest request) {
        service.excludeAndUpdate(accountKey, request.patch(), request.reason(), request.userId());
        return Map.of("accountKey", accountKey, "status", "MANUAL");
    }

    @PostMapping("/{accountKey}/reinclude")
    public Map<String, Object> reinclude(@PathVariable String accountKey,
                                         @RequestBody(required = false) ReincludeRequest request) {
        Long sourceId = request == null ? null : request.sourceId();
        String userId = request == null ? "system" : request.userId();
        service.reinclude(accountKey, sourceId, userId);
        return Map.of("accountKey", accountKey, "status", "SYNC");
    }

    public record ExcludeRequest(String reason, String userId) {
    }

    public record ManualUpdateRequest(String reason, String userId, Map<String, Object> patch) {
    }

    public record ReincludeRequest(Long sourceId, String userId) {
    }
}
