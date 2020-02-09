package com.epeirogenic.kfk;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

    private final RunService runService;

    Controller(final @Autowired RunService runService) {
        this.runService = runService;
    }

    @PostMapping(path = "/send/")
    public String sendFoo() {

        return runService.run();
    }
}
