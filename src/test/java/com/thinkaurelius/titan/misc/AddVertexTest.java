package com.thinkaurelius.titan.misc;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.tinkerpop.blueprints.Vertex;

public class AddVertexTest {

    private final Logger LOG = LoggerFactory.getLogger(AddVertexTest.class);
    private static final String MASTER_ID = "master_id";
    private TitanGraph titanGraph;

    @Before
    public void setup() throws IOException, StorageException {

        String titanGraphDir = "target/bdb";
        FileUtils.deleteQuietly(new File(titanGraphDir));
        titanGraph = createTitanGraph(berkleyGraphConfiguration(titanGraphDir));

//        titanGraph = createTitanGraph(cassandraGraphConfiguration());
//        ((StandardTitanGraph)titanGraph).clear();

        makeTypes();
    }

    private TitanGraph createTitanGraph(Configuration conf) {
        return TitanFactory.open(conf);
    }

    private static Configuration berkleyGraphConfiguration(String titanGraphDir) {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty("storage.directory", titanGraphDir);
        conf.setProperty("autotype", "none");
        return conf;
    }

    private static Configuration cassandraGraphConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty("storage.backend", "cassandrathrift");
        conf.setProperty("storage.hostname", "127.0.0.1");
        conf.setProperty("autotype", "none");
        return conf;
    }

    private void makeTypes() {
        titanGraph.makeKey(MASTER_ID)
                .dataType(Integer.class)
                .unique()
                .indexed(Vertex.class)
                .make();
        titanGraph.commit();
    }

    @Test
    public void test() throws InterruptedException {
        final List<Integer> masterIdsToLoad = getMasterIds(5, 100);
        final List<Thread> threads = createWorkerThreads(5, masterIdsToLoad);
        startThreads(threads);
        joinThreads(threads);

        //check
        checkMasterIDs(titanGraph);

    }

    private List<Thread> createWorkerThreads(int numThreads, List<Integer> masterIdsToLoad) {
        final ImmutableList.Builder<Thread> threads = ImmutableList.builder();
        for (int i = 0; i < numThreads; ++i) {
            threads.add(new Thread(new Worker(masterIdsToLoad)));
        }
        return threads.build();
    }

    private void startThreads(List<Thread> threads) {
        for (Thread thread : threads) {
            thread.start();
        }
    }

    private void joinThreads(List<Thread> threads) throws InterruptedException {
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private void checkMasterIDs(TitanGraph titanGraph) {
        final Iterable<Vertex> vertices = titanGraph.getVertices();
        titanGraph.commit(); // read commit
        final Iterable<Integer> masterIds = Iterables.transform(vertices, new Function<Vertex, Integer>() {
            @Override
            public Integer apply(Vertex vertex) {
                return vertex.getProperty(MASTER_ID);
            }
        });
        LOG.info("masterIds: " + masterIds);
        List<Integer> masterIDList = Ordering.natural().sortedCopy(masterIds);

        ImmutableList.Builder<Integer> b = ImmutableList.builder();
        for (int i = 1; i <= 5; i++)
            b.add(i);

        assertEquals(b.build(), masterIDList);
    }

    private List<Integer> getMasterIds(int n, int times) {
        final ImmutableList.Builder<Integer> masterIds = ImmutableList.builder();
        for (int masterId = 1; masterId <= n; ++masterId) {
            masterIds.addAll(singleValueSequence(masterId, times));
        }
        return masterIds.build();
    }

    private <T> Iterable<T> singleValueSequence(T value, int times) {
        ImmutableList.Builder<T> sequence = ImmutableList.builder();
        for (int i = 0; i < times; ++i) {
            sequence.add(value);
        }
        return sequence.build();
    }

    private class Worker implements Runnable {

        private final Logger LOG = LoggerFactory.getLogger(Worker.class);
        private final List<Integer> masterIdsToLoad;

        public Worker(List<Integer> masterIdsToLoad) {
            this.masterIdsToLoad = masterIdsToLoad;
        }

        @Override
        public void run() {
            LOG.info("--- worker start ---");
            for (Integer masterId : masterIdsToLoad) {
                try {
                    final Vertex vertex = titanGraph.addVertex(null);
                    vertex.setProperty(MASTER_ID, masterId);
                    titanGraph.commit();
                    LOG.info("added vertex with master_id={}", masterId);
                } catch (Exception e) {
                    titanGraph.rollback();
                    LOG.error("cannot add vertex with master_id={}", masterId, e);
                }
            }
            LOG.info("--- worker stop ---");
        }
    }
}
