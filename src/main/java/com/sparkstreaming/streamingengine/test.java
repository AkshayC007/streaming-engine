package com.sparkstreaming.streamingengine;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class test {

    @RequestMapping("/testing")
    public String test(){
        return "Hello";
    }
}
