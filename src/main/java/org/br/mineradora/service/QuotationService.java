package org.br.mineradora.service;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.br.mineradora.client.CurrencyPriceClient;
import org.br.mineradora.dto.CurrencyPriceDTO;
import org.br.mineradora.dto.QuotationDTO;
import org.br.mineradora.entity.QuotationEntity;
import org.br.mineradora.message.KafkaEvents;
import org.br.mineradora.repository.QuotationRepository;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QuotationService {

	@Inject
	@RestClient
	CurrencyPriceClient currencyPriceClient;
	
	@Inject
	QuotationRepository quotationRepository;
	
	@Inject
	KafkaEvents kafkaEvents;
	
	public void getCurrencyPrice() {
		
		CurrencyPriceDTO currencyPriceInfo = currencyPriceClient.getPriceByPair("USD-BRL");
		
		if(updateCurrencyInfoPrice(currencyPriceInfo)) {
			kafkaEvents.sendNewKafkaEvent(QuotationDTO
					.builder()
					.currencyPrice(new BigDecimal(currencyPriceInfo.getUSDBRL().getBid()))
					.date(new Date())
					.build());
		}
	}
	
	public boolean updateCurrencyInfoPrice(CurrencyPriceDTO currencyPriceInfo) {
		BigDecimal currentPrice = new BigDecimal(currencyPriceInfo.getUSDBRL().getBid());
		AtomicBoolean updatePrice = new AtomicBoolean(false);
		
		List<QuotationEntity> quotationList = quotationRepository.findAll().list();
		
		if(quotationList.isEmpty()) {
			saveQuotation(currencyInfo);
			updatePrice.set(true);
		}
		else {
			QuotationEntity lastDollarPrice = quotationList.get(quotationList.size() - 1);
			
			if(currentPrice.floatValue() > lastDollarPrice.getCurrencyPrice().floatValue()) {
				updatePrice.set(true);
				saveQuotation(currencyInfo);
			}
		}
		
		//TODO saveQuotation method
		return updatePrice.get();
	}
}
