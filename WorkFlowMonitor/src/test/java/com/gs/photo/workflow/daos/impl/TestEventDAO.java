package com.gs.photo.workflow.daos.impl;

import java.util.Arrays;

import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.mock.mockito.MockBean;

import com.gs.photo.workflow.daos.IEventDAO;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventProduced;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;

// @RunWith(SpringRunner.class)
//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
//@SpringBootTest(classes = { ApplicationConfig.class, EventDAO.class })
@Ignore
class TestEventDAO {

    @Autowired
    protected IEventDAO eventDAO;

    @Before
    public void setUp() throws Exception { MockitoAnnotations.initMocks(this); }

    @MockBean
    @Qualifier("producerForPublishingWfEvents")
    protected Producer<String, WfEvents> producerForPublishingWfEvents;

    void testCreateEvent() {
        this.eventDAO.truncate();
        WfEvents events = WfEvents.builder()
            .withProducer("me")
            .withEvents(
                Arrays.asList(
                    new WfEvent[] {
                            WfEventProduced.builder()
                                .withDataId("DATA_ID_1")
                                .withImgId("IMG_ID")
                                .withParentDataId("DATA_ID_1")
                                .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_ARCHIVED_IN_HDFS)
                                .build(),
                            WfEventProduced.builder()
                                .withDataId("DATA_ID_2")
                                .withImgId("IMG_ID")
                                .withParentDataId("DATA_ID_2")
                                .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_ARCHIVED_IN_HDFS)
                                .build() }))
            .build();
        this.eventDAO.addOrCreate(events);
        Assert.assertEquals(2, this.eventDAO.getNbOfEvents());
    }

}