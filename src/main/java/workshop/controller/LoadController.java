package workshop.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import workshop.model.Column;
import workshop.model.Table;
import workshop.service.DatabaseService;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Controller
public class LoadController {

    private final DatabaseService databaseService;


    public LoadController(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

/*    @GetMapping("/hi")
    public String getSourceDatabaseDetails() {
        databaseService.getSourceDatabaseInfo();

   *//*     Optional<Table> targetTableName = sourceTables.stream()
                .filter(table -> table.tableName().equals("EMPLOYEES"))
                .findFirst();*//*

 *//*       if (targetTableName.isPresent()) {
            Table table = targetTableName.get();
            List<Map<Column, Object>> tableRows = databaseService.getTableRows(table, "");
            //tableRows.forEach(map -> log.info(map.values().toString()));
            databaseService.persistTableRows(tableRows, table);
        }*//*
        return "indexaa";
    }*/

    @GetMapping("/")
    public String homePage() {
        databaseService.getSourceDatabaseInfo();
        databaseService.getTargetDatabaseInfo();
        return "index";
    }
}
