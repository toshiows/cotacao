package org.br.mineradora.scheduler;

import org.br.mineradora.service.QuotationService;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class QuotationScheduler {

	@Inject
	QuotationService quotationService;
	
	@Transactional
	@Scheduled(every = "35s", identity = "task-job")
	void schedule() {
		quotationService.getCurrencyPrice();
	}
}
