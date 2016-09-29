/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jassoft.markets.analyse;

import com.jassoft.markets.datamodel.sources.Source;
import com.jassoft.markets.datamodel.story.NamedEntities;
import com.jassoft.markets.datamodel.story.Story;
import com.jassoft.markets.datamodel.story.StoryPredicates;
import com.jassoft.markets.datamodel.story.metric.Metric;
import com.jassoft.markets.datamodel.story.metric.MetricBuilder;
import com.jassoft.markets.datamodel.system.Queue;
import com.jassoft.markets.repository.SourceRepository;
import com.jassoft.markets.repository.StoryRepository;
import com.jassoft.markets.utils.SourceUtils;
import com.jassoft.markets.utils.lingual.NamedEntityRecognizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.Date;


/**
 *
 * @author Jonny
 */
@Component
public class StoryIndexerListener implements MessageListener
{
    private static final Logger LOG = LoggerFactory.getLogger(StoryIndexerListener.class);

    @Autowired
    private StoryRepository storyRepository;

    @Autowired
    private SourceRepository sourceRepository;

    @Autowired
    protected MongoTemplate mongoTemplate;

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    @Qualifier("NLP_Story")
    private NamedEntityRecognizer namedEntityRecognizer;

    void send(final String message) {
        jmsTemplate.convertAndSend(Queue.IndexedStory.toString(), message);
    }
    
    @Override
    @JmsListener(destination = "StoriesWithContent", concurrency = "5")
    public void onMessage( final Message message )
    {
        final Date start = new Date();
        if ( message instanceof TextMessage )
        {
            final TextMessage textMessage = (TextMessage) message;
            try
            {                
                Story story = storyRepository.findOne(textMessage.getText());
                
                if(story == null)
                    return;
                
                Source source = sourceRepository.findOne(story.getParentSource());
                               
                if(SourceUtils.matchesExclusion(source.getExclusionList(), story.getUrl().toString()))
                {
                    LOG.warn("Duplicate Story or URL in exclusion List [{}] Stopping processing at [{}]", story.getUrl().toString(), this.getClass().getName());
                    storyRepository.delete(story.getId());
                    return;
                }

                if(StoryPredicates.isStoryEmpty().test(story))
                {
                    LOG.warn("Story [{}] Has no body Stopping processing at [{}]", story.getUrl().toString(), this.getClass().getName());
                    storyRepository.delete(story.getId());
                    return;
                }
                
                NamedEntities entities = namedEntityRecognizer.analyseStory(story.getBody());

                Metric metric = MetricBuilder.anAnalyseMetric().withStart(start).withEndNow().withDetail(String.format("Found %s Company names", entities.getOrganisations().size())).build();
                mongoTemplate.updateFirst(Query.query(Criteria.where("id").is(story.getId())), new Update().set("entities", entities).push("metrics", metric), Story.class);
                send(story.getId());
            }
            catch (final Exception exception)
            {
                LOG.error(exception.getLocalizedMessage(), exception);
                
                throw new RuntimeException(exception);
            }
        }
    }
}
