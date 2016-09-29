package uk.co.jassoft.markets.analyse;

import uk.co.jassoft.markets.datamodel.sources.SourceBuilder;
import uk.co.jassoft.markets.datamodel.story.Story;
import uk.co.jassoft.markets.datamodel.story.StoryBuilder;
import uk.co.jassoft.markets.repository.SourceRepository;
import uk.co.jassoft.markets.repository.StoryRepository;
import uk.co.jassoft.markets.utils.lingual.NamedEntityRecognizer;
import uk.co.jassoft.utils.BaseRepositoryTest;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.jms.TextMessage;
import java.net.URL;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by jonshaw on 18/03/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = SpringConfiguration.class)
@IntegrationTest
public class StoryIndexerListenerTest extends BaseRepositoryTest {

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

    @Autowired
    private StoryIndexerListener target;

    @Test
    public void onMessage_storyMatchesSourceExclusion_isDeleted() throws Exception {

        String sourceId = sourceRepository.save(SourceBuilder.aSource()
                .withExclusionList(Arrays.asList("test.com"))
                .build())
                .getId();

        String storyId = storyRepository.save(new StoryBuilder()
                .setParentSource(sourceId)
                .setUrl(new URL("http://test.com"))
                .createStory())
                .getId();

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(storyId);

        target.onMessage(textMessage);

        assertEquals(0, storyRepository.count());
    }

    @Test
    public void onMessage_storyWithEmptyBody_isDeleted() throws Exception {

        String sourceId = sourceRepository.save(SourceBuilder.aSource()
                .withExclusionList(Arrays.asList())
                .build())
                .getId();

        String storyId = storyRepository.save(new StoryBuilder()
                .setParentSource(sourceId)
                .setUrl(new URL("http://test.com"))
                .createStory())
                .getId();

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(storyId);

        target.onMessage(textMessage);

        assertEquals(0, storyRepository.count());
    }



    @Test
    public void onMessage_storyWithBody_generatesNamedEntities() throws Exception {

        String sourceId = sourceRepository.save(SourceBuilder.aSource()
                .withExclusionList(Arrays.asList())
                .build())
                .getId();

        String storyId = storyRepository.save(new StoryBuilder()
                .setParentSource(sourceId)
                .setUrl(new URL("http://test.com"))
                .setBody("Markets | Thu Mar 17, 2016 8:44am EDT\n" +
                        "Dollar dives as Fed pulls in rate hike horns\n" +
                        "LONDON | By Marc Jones\n" +
                        "A man walks through the lobby of the London Stock Exchange in London, Britain August 25, 2015.\n" +
                        "Reuters/Suzanne Plunkett\n" +
                        "LONDON The dollar tumbled on Thursday, lifting world shares to their highest level of the year, after the Federal Reserve scaled down its own expectations of the number of U.S. rate hikes likely over the next nine months.\n" +
                        "The Fed, via its 'dot plot' system, which charts what rate moves policymakers expect, effectively chopped those forecasts in half, from four hikes to two in a nod to the \"global risks\" casting a cloud over the world's largest economy.\n" +
                        "It was a signal that triggered a slump in the dollar and a surge in risk appetite that rolled from Wall Street to Asia and then into Europe, although shares there, especially exporters winced .FTEU3 as the euro EUR= jumped.\n" +
                        "London .FTSE was off a modest 0.2 percent bolstered by a near 5 percent jump in mining firms, but Frankfurt .GDAXI and Paris .FCHI were down 1.8 and 1.6 percent and Wall Street was expected to open down too having hit a 2016 high on Wednesday.\n" +
                        "Commodity markets continued to cheer however. Brent oil jumped above $41 a barrel as a number of large producers also nailed down a date for an output freeze meeting. Industrial metals such s copper CMCU3 saw their biggest rise in two weeks.\n" +
                        "But it was the currency markets that really grabbed the attention as the dollar sank to 1-1/2 year and one-month lows against yen JPY= and the euro EUR= respectively, and emerging market and oil and commodity-linked currencies surged.\n" +
                        "\"Risk is thoroughly on,\" said Societe Generale global head of currency strategy Kit Juckes. \"All the chit-chat was that they (the Fed) were going to be hawkish, and they weren't.\"\n" +
                        "\"The dollar is obviously the loser, but it's good for shares, it's good for oil, and good for debt too, I would say.\"\n" +
                        "World growth concerns, particularly regarding China, have rattled markets through much of this year, and this was seen to have influenced the Fed's shift in position as it cited the \"global risks\" facing the U.S. economy.\n" +
                        "The dollar showed was showing no sign of stabilizing as U.S. trading began, having sliced all the way down to 110.77 yen and catapulted the euro above $1.13 EUR= for the first time since mid-January.\n" +
                        "Currency traders were suddenly betting the greenback rather than the euro will be heading south and commodity-linked currencies rose strongly as products such as oil and iron ore also soared on Fed hopes.\n" +
                        "The Australian dollar, which had already jumped 1.2 percent overnight, caught a fresh lift from an upbeat local jobs report and rose to an eight-month high of $0.7620 AUD=D4 .\n" +
                        "The Canadian dollar was firm at just under C$1.30 to the U.S. dollar CAD=D4 after rallying nearly 2 percent to a four-month peak of C$1.3094 overnight.\n" +
                        "SURGING EMERGING\n" +
                        "Despite Europe's .FTEU3 reversal, MSCI's 46-country All World share index .MIWD00000PUS climbed over 0.8 percent on the day to reach its highest since Jan 4., the opening trading day of 2016 for most major markets.\n" +
                        "For emerging markets, the news was even better as a more than 2 percent surge took the volatile asset class's stocks MSCIEF to their highest since mid-December and currencies and debt rallied too.\n" +
                        "One outlier was South Africa, though, ahead of a meeting of its central bank after another week in which the rand has been hammered by political worries.\n" +
                        "In the latest twist, South Africa's deputy finance minister received a death threat shortly before he publicly accused a wealthy family with links to President Jacob Zuma of offering him the job of finance minister, a newspaper said on Thursday.\n" +
                        "Zuma has previously said his ties with the family are above-board. His office was not immediately available to comment, although he is due to answer questions in parliament on Thursday.\n" +
                        "The Malaysian ringgit MYR= , Indonesian rupiah IDR= and South Korean won KRW= all rose more than 1 percent against the dollar as a clutch of Asian currencies hit multi-month peaks.\n" +
                        "\"In the past, when the dollar weakened after the Fed was dovish, the dollar weakness lasted for maybe about three to four months,\" said Tan Teck Leng, FX strategist for UBS chief investment office Wealth Management in Singapore.\n" +
                        "\"But is this the end of the strong dollar? We don't think so,\" he said, adding that the Fed could start sounding hawkish again around June and July to pave the way for a rate rise, possibly in September.\n" +
                        "MSCI's broadest index of Asia-Pacific shares outside Japan .MIAPJ0000PUS climbed to a two-month high as Australian stocks  added 1 percent, South Korea's Kospi .KS11 rose 0.9 percent and Shanghai .SSEC was up 1 percent.\n" +
                        "The jump in the yen meant Japan's Nikkei .N225 lost out though, as it closed down 0.2 percent.\n" +
                        "Oil prices, the other main driver of global market sentiment in recent months, rose to a three-month peak of $39.54 a barrel CLc1 after surging nearly 6 percent overnight. Brent LCOc1 was up 95 cents at $41.27 a barrel.\n" +
                        "Three-month copper on the London Metal Exchange CMCU3 traded up 1.5 percent at $5,065 a tonne. A weaker greenback tends to favor commodities traded in dollars by making them cheaper for non-U.S. buyers.\n" +
                        "(Reporting by Marc Jones; Editing by Jon Boyle)\n")
                .createStory())
                .getId();

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(storyId);

        target.onMessage(textMessage);

        assertEquals(1, storyRepository.count());

        final Story result = storyRepository.findOne(storyId);

        assertNotNull(result.getEntities());

        assertNotNull(result.getEntities().getOrganisations());
        assertEquals(25, result.getEntities().getOrganisations().size());

        assertNotNull(result.getEntities().getPeople());
        assertEquals(13, result.getEntities().getPeople().size());

        assertNotNull(result.getEntities().getMisc());
        assertEquals(10, result.getEntities().getMisc().size());

        assertNotNull(result.getEntities().getLocations());
        assertEquals(17, result.getEntities().getLocations().size());
    }
}