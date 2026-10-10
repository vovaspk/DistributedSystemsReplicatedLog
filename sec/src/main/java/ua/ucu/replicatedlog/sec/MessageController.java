package ua.ucu.replicatedlog.sec;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import ua.ucu.replicatedlog.core.Message;
import ua.ucu.replicatedlog.core.SecondaryNode;

@RestController
public class MessageController {

    private final SecondaryNode secondary;

    public MessageController(SecondaryNode secondary) {
        this.secondary = secondary;
    }

    /** Internal endpoint used by the master; the response is the ACK. */
    @PostMapping("/replicate")
    @ResponseStatus(HttpStatus.OK)
    public void replicate(@RequestBody Message message) {
        secondary.onReplicate(message);
    }

    @GetMapping("/messages")
    public List<String> list() {
        return secondary.listMsgs();
    }
}
