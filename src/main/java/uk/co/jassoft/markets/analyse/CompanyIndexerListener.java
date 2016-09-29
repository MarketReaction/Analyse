/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package uk.co.jassoft.markets.analyse;

import uk.co.jassoft.markets.datamodel.company.Company;
import uk.co.jassoft.markets.datamodel.company.Exchange;
import uk.co.jassoft.markets.datamodel.story.NamedEntities;
import uk.co.jassoft.markets.repository.CompanyRepository;
import uk.co.jassoft.markets.repository.ExchangeRepository;
import uk.co.jassoft.markets.utils.lingual.NamedEntityRecognizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;


/**
 *
 * @author Jonny
 */
@Component
public class CompanyIndexerListener implements MessageListener
{
    private static final Logger LOG = LoggerFactory.getLogger(CompanyIndexerListener.class);

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private ExchangeRepository exchangeRepository;

    @Autowired
    protected MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier("NLP_Company")
    private NamedEntityRecognizer namedEntityRecognizer;

    @Override
    @JmsListener(destination = "CompanyWithInformation", concurrency = "5")
    public void onMessage( final Message message )
    {
        if ( message instanceof TextMessage )
        {
            final TextMessage textMessage = (TextMessage) message;
            try
            {
                Company company = companyRepository.findOne(textMessage.getText());
                Exchange exchange = exchangeRepository.findOne(company.getExchange());

                if(company == null)
                    return;

                NamedEntities entities;

                if(company.getCompanyInformation() == null || company.getCompanyInformation().isEmpty()) {
                    entities = new NamedEntities();
                }
                else {
                    LOG.info("Running Analysis For Company [{}]", company.getName());

                    entities = namedEntityRecognizer.analyseCompany(company.getCompanyInformation());
                }

                namedEntityRecognizer.addOrIncrement(entities.getOrganisations(), company.getName());
                namedEntityRecognizer.addOrIncrement(entities.getOrganisations(), company.getTickerSymbol());
                namedEntityRecognizer.addOrIncrement(entities.getOrganisations(), String.format("%s:%s", exchange.getCode(), company.getTickerSymbol()));

                mongoTemplate.updateFirst(Query.query(Criteria.where("id").is(company.getId())), Update.update("entities", entities), Company.class);
            }
            catch (final Exception exception)
            {
                LOG.error(exception.getLocalizedMessage(), exception);
                
                throw new RuntimeException(exception);
            }
        }
    }       
}
