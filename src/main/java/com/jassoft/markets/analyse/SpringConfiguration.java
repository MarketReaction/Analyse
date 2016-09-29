package com.jassoft.markets.analyse;

import com.jassoft.markets.BaseSpringConfiguration;
import com.jassoft.markets.utils.lingual.NamedEntityRecognizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * Created by jonshaw on 13/07/15.
 */
@Configuration
@ComponentScan("com.jassoft.markets.analyse")
public class SpringConfiguration extends BaseSpringConfiguration {

    @Bean
    @Qualifier("NLP_Story")
    public NamedEntityRecognizer storyNamedEntityRecognizer() {
        return new NamedEntityRecognizer("StanfordCoreNLP_Story");
    }

    @Bean(name = "NLP_Company")
    public NamedEntityRecognizer companyNamedEntityRecognizer() {
        return new NamedEntityRecognizer("StanfordCoreNLP_Company");
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(SpringConfiguration.class, args);
    }
}
