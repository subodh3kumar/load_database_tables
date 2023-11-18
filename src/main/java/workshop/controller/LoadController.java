package workshop.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import workshop.model.Column;
import workshop.model.Table;
import workshop.service.SourceService;
import workshop.service.TargetService;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Controller
public class LoadController {

    private final SourceService sourceService;
    private final TargetService targetService;

    public LoadController(SourceService sourceService, TargetService targetService) {
        this.sourceService = sourceService;
        this.targetService = targetService;
    }

    @GetMapping("/")
    public String getSourceDatabaseDetails() {
        List<Table> sourceTables = sourceService.getSourceDatabaseInfo();

        Optional<Table> targetTableName = sourceTables.stream()
                .filter(table -> table.tableName().equals("EMPLOYEES"))
                .findFirst();

        if (targetTableName.isPresent()) {
            Table table = targetTableName.get();
            List<Map<Column, Object>> tableRows = sourceService.getTableRows(table, "");
            //tableRows.forEach(map -> log.info(map.values().toString()));
            targetService.persistTableRows(tableRows, table);
        }
        return "index";
    }
}
