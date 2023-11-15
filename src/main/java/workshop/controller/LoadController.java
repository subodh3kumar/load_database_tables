package workshop.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import jakarta.servlet.http.HttpSession;
import workshop.service.SourceService;

@Controller
public class LoadController {

	private final SourceService sourceDatabaseService;

	public LoadController(SourceService sourceDatabaseService) {
		this.sourceDatabaseService = sourceDatabaseService;
	}

	@GetMapping("/")
	public String getSourceDatabaseDetails(HttpSession session) {
		sourceDatabaseService.getSourceDatabaseInfo(session);

		sourceDatabaseService.getTableRows();
		return "index";
	}
}
