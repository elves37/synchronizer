package com.hi.usersync.api;

import com.hi.usersync.sync.SyncRunRepository;
import com.hi.usersync.sync.SyncRunView;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/sync-runs")
public class SyncRunController {
    private final SyncRunRepository repository;

    public SyncRunController(SyncRunRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public List<SyncRunView> list(@RequestParam(required = false) Long sourceId,
                                  @RequestParam(defaultValue = "100") int limit) {
        return repository.findRuns(sourceId, limit);
    }

    @GetMapping("/{id}")
    public SyncRunView get(@PathVariable Long id) {
        return repository.findById(id).orElseThrow(() -> new IllegalArgumentException("Unknown run id: " + id));
    }
}
