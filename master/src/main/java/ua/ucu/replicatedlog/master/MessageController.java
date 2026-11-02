package ua.ucu.replicatedlog.master;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import ua.ucu.replicatedlog.core.MasterNode;

@RestController
public class MessageController {

    private final MasterNode master;

    public MessageController(MasterNode master) {
        this.master = master;
    }

    @PostMapping("/messages")
    @ResponseStatus(HttpStatus.CREATED)
    public void append(@RequestBody String message, @RequestParam(name = "w", required = false) Integer w) {
        if (w == null) {
            master.appendMsg(message);
        } else {
            master.appendMsg(message, w);
        }
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String badWriteConcern(IllegalArgumentException e) {
        return e.getMessage();
    }

    @GetMapping("/messages")
    public List<String> list() {
        return master.listMsgs();
    }
}
