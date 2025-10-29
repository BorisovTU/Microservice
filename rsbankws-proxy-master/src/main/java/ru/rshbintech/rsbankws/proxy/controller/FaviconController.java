package ru.rshbintech.rsbankws.proxy.controller;

import io.swagger.v3.oas.annotations.Hidden;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Hidden
@Controller
@SuppressWarnings("unused")
public class FaviconController {

    @ResponseBody
    @GetMapping("favicon.ico")
    public void returnNoFavicon() {
        //Переопределение контроллера favicon для вызова технических endpoint'ов из браузера для обхода ошибки 404
    }

}
