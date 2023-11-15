package workshop.model;

import java.util.List;

public record Table(String tableName, List<Column> columns) {

}
