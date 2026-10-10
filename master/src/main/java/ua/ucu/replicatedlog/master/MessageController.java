package ua.ucu.replicatedlog.master;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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
    public void append(@RequestBody String message) {
        master.appendMsg(message);
    }

    @GetMapping("/messages")
    public List<String> list() {
        return master.listMsgs();
    }
}
