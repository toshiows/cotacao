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
	
	private static final String PAIR_USD_BRL = "USD-BRL";
	
	public void getCurrencyPrice() {
		
		CurrencyPriceDTO currencyPriceInfo = currencyPriceClient.getPriceByPair(PAIR_USD_BRL);
		
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
			saveQuotation(currencyPriceInfo);
			updatePrice.set(true);
		}
		else {
			QuotationEntity lastDollarPrice = quotationList.get(quotationList.size() - 1);
			
			if(currentPrice.floatValue() > lastDollarPrice.getCurrencyPrice().floatValue()) {
				updatePrice.set(true);
				saveQuotation(currencyPriceInfo);
			}
		}
		
		return updatePrice.get();
	}
	
	private void saveQuotation(CurrencyPriceDTO currencyInfo) {
		QuotationEntity quotation = new QuotationEntity();
		quotation.setDate(new Date());
		quotation.setCurrencyPrice(new BigDecimal(currencyInfo.getUSDBRL().getBid()));
		quotation.setPctChange(currencyInfo.getUSDBRL().getPctChange());
		quotation.setPair(PAIR_USD_BRL);
		
		quotationRepository.persist(quotation);
	}
}
