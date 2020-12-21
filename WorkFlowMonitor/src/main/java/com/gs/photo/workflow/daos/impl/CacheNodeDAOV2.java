package com.gs.photo.workflow.daos.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Streams;
import com.gs.photo.workflow.daos.ICacheNodeDAO;
import com.nurkiewicz.typeof.TypeOf;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventCopy;
import com.workflow.model.events.WfEventFinal;
import com.workflow.model.events.WfEventInitial;
import com.workflow.model.events.WfEventProduced;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;

public class CacheNodeDAOV2 implements ICacheNodeDAO<String, WfEvents> {

    protected static Logger                                           LOOGER = LoggerFactory
        .getLogger(CacheNodeDAOV2.class);

    ConnectivityInspector<WfEvent, DefaultEdge>                       connectivityInspector;

    protected Map<String, DefaultDirectedGraph<WfEvent, DefaultEdge>> graphs;
    protected Set<String>                                             completeImages;

    @Override
    @PostConstruct
    public void init() {
        this.graphs = new ConcurrentHashMap<>();
        this.completeImages = new HashSet<>();
    }

    public void createCompleteGraph() {}

    @Override
    public WfEvents addOrCreate(String key, WfEvents events) {
        final Function<WfEvent, Boolean> mapper = (evt) -> this.addOrCreate(evt);
        final long count = events.getEvents()
            .stream()
            .map(mapper)
            .filter((c) -> c)
            .count();
        if (count != events.getEvents()
            .size()) {
            CacheNodeDAOV2.LOOGER.warn("[CACHENODE][{}] events not processed {} !! ", key, events);
        }
        return events;
    }

    private <T extends WfEvent> boolean addOrCreate(T event) {
        DefaultDirectedGraph<WfEvent, DefaultEdge> graph = this.graphs.computeIfAbsent(
            event.getImgId(),
            (a) -> new DefaultDirectedGraph<WfEvent, DefaultEdge>(DefaultEdge.class));
        if (this.completeImages.contains(event.getImgId())) {
            CacheNodeDAOV2.LOOGER.warn("[CACHENODE][{}] Unexepected event : {} !! ", event.getImgId(), event);
            return false;
        } else {
            return TypeOf.whenTypeOf(event)
                .is(WfEventInitial.class)
                .thenReturn((e) -> this.processEvent(graph, e))
                .is(WfEventCopy.class)
                .thenReturn((e) -> this.processEvent(graph, e))
                .is(WfEventFinal.class)
                .thenReturn((e) -> this.processEvent(graph, e))
                .is(WfEventRecorded.class)
                .thenReturn((e) -> this.processLastEvent(graph, e))
                .is(WfEventProduced.class)
                .thenReturn((e) -> this.processEvent(graph, e))
                .get();
        }
    }

    protected boolean addToVertex(DefaultDirectedGraph<WfEvent, DefaultEdge> graph, WfEvent e) {
        return graph.addVertex(e);
    }

    private boolean processLastEvent(DefaultDirectedGraph<WfEvent, DefaultEdge> graph, WfEvent lastReceivedEvent) {
        this.addToVertex(graph, lastReceivedEvent);
        Optional<WfEvent> retValue = graph.vertexSet()
            .stream()
            .filter(
                (evt) -> lastReceivedEvent.getParentDataId()
                    .equalsIgnoreCase(evt.getDataId()))
            .map((evt) -> {
                this.addEdge(graph, evt, lastReceivedEvent);
                return evt;
            })
            .findFirst();
        DepthFirstIterator<WfEvent, DefaultEdge> graphIterator = new DepthFirstIterator<>(graph);
        Optional<WfEvent> listOfEvents = Streams.stream(graphIterator)
            .filter((we) -> we instanceof WfEventInitial)
            .findFirst()
            .map((we) -> {
                this.addEdge(graph, lastReceivedEvent, we);
                return we;
            });
        return true;
    }

    private boolean processEvent(DefaultDirectedGraph<WfEvent, DefaultEdge> graph, WfEvent lastReceivedEvent) {
        this.addToVertex(graph, lastReceivedEvent);
        Optional<WfEvent> retValue = graph.vertexSet()
            .stream()
            .filter(
                (evt) -> lastReceivedEvent.getParentDataId()
                    .equalsIgnoreCase(evt.getDataId()))
            .map((evt) -> {
                this.addEdge(graph, evt, lastReceivedEvent);
                return evt;
            })
            .findFirst();
        DepthFirstIterator<WfEvent, DefaultEdge> graphIterator = new DepthFirstIterator<>(graph);
        List<WfEvent> listOfEvents = Streams.stream(graphIterator)
            .filter((we) -> (graph.inDegreeOf(we) == 0) && !(we instanceof WfEventInitial))
            .filter(
                (we) -> lastReceivedEvent.getDataId()
                    .equalsIgnoreCase(we.getParentDataId()))
            .collect(Collectors.toList());
        listOfEvents.forEach((evt) -> { this.addEdge(graph, lastReceivedEvent, evt); });

        return true;
    }

    protected DefaultEdge addEdge(DefaultDirectedGraph<WfEvent, DefaultEdge> graph, WfEvent event, WfEvent evt) {
        return graph.addEdge(evt, event);
    }

    @Override
    public boolean allEventsAreReceivedForAnImage(String imgId) {
        final DefaultDirectedGraph<WfEvent, DefaultEdge> defaultDirectedGraph = this.graphs.get(imgId);
        DepthFirstIterator<WfEvent, DefaultEdge> graphIterator = new DepthFirstIterator<>(defaultDirectedGraph);
        boolean retValue = false;
        graphIterator = new DepthFirstIterator<>(defaultDirectedGraph);
        long nbOfArchiveElements = Streams.stream(graphIterator)
            .filter(
                (t) -> t.getStep()
                    .equals(WfEventStep.WF_STEP_CREATED_FROM_STEP_ARCHIVED_IN_HDFS))
            .count();

        if (nbOfArchiveElements > 0) {
            CacheNodeDAOV2.LOOGER.info("[CACHENODE][{}] after receiving archive event ", imgId);
            retValue = Streams.stream(graphIterator)
                .allMatch(
                    (we) -> TypeOf.whenTypeOf(we)
                        .is(WfEventInitial.class)
                        .thenReturn((e) -> {
                            CacheNodeDAOV2.LOOGER.info(
                                "[CACHENODE][{}] checking initial event  {} nbOfInitEvents/ {} nbOfCreatedEvent  ",
                                imgId,
                                e.getNbOfInitialEvents(),
                                defaultDirectedGraph.outDegreeOf(e));
                            return defaultDirectedGraph.outDegreeOf(e) == e.getNbOfInitialEvents();
                        })
                        .is(WfEventFinal.class)
                        .thenReturn((e) -> {
                            CacheNodeDAOV2.LOOGER.info(
                                "[CACHENODE][{}] checking final event  {} nbOfExpectedEvents/ {} nbOfCreatedEvent  ",
                                imgId,
                                e.getNbOFExpectedEvents(),
                                defaultDirectedGraph.inDegreeOf(e));
                            return defaultDirectedGraph.inDegreeOf(e) == e.getNbOFExpectedEvents();
                        })
                        .is(WfEventRecorded.class)
                        .thenReturn((e) -> defaultDirectedGraph.inDegreeOf(e) == 1)
                        .is(WfEventCopy.class)
                        .thenReturn(
                            (e) -> (defaultDirectedGraph.inDegreeOf(e) == 1)
                                && (defaultDirectedGraph.outDegreeOf(e) == 1))
                        .is(WfEventProduced.class)
                        .thenReturn(
                            (e) -> (defaultDirectedGraph.outDegreeOf(e) > 0)
                                && (defaultDirectedGraph.inDegreeOf(e) == 1))
                        .orElse(false));
            if (retValue) {
                this.graphs.remove(imgId);
                this.completeImages.add(imgId);
            } else {
                CacheNodeDAOV2.LOOGER.info("[CACHENODE][{}] Archive element arrived but graph not completed...", imgId);
            }
        }

        return retValue;
    }

    public void display(String imgId) {
        final DefaultDirectedGraph<WfEvent, DefaultEdge> defaultDirectedGraph = this.graphs.get(imgId);
        DepthFirstIterator<WfEvent, DefaultEdge> graphIterator = new DepthFirstIterator<>(defaultDirectedGraph);
        Streams.stream(graphIterator)
            .forEach(
                (x) -> CacheNodeDAOV2.LOOGER.info(
                    "--> " + x + "\n  out : " + defaultDirectedGraph.outgoingEdgesOf(x) + "\n  in  : "
                        + defaultDirectedGraph.incomingEdgesOf(x)));

    }

}
