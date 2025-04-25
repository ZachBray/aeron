package io.aeron.cluster;

import io.aeron.cluster.service.Cluster;
import io.aeron.test.*;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import net.bytebuddy.asm.Advice;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.IntFunction;

import static io.aeron.test.cluster.TestCluster.aCluster;
import static org.junit.jupiter.api.Assertions.*;

@SlowTest
@ExtendWith({EventLogExtension.class, InterruptingTestCallback.class})
class ShutdownElectionRaceTest
{
    private static ClusterInstrumentor clusterInstrumentor1;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    final CountDownLatch[] signalTerminateLatch = new CountDownLatch[] {
        new CountDownLatch(1),
        new CountDownLatch(1),
        new CountDownLatch(1)
    };

    final CountDownLatch[] beforeTerminateAckLatch = new CountDownLatch[] {
        new CountDownLatch(1),
        new CountDownLatch(1),
        new CountDownLatch(1)
    };

    private final IntFunction<TestNode.TestService[]> serviceSupplier =
        (i) -> new TestNode.TestService[] {new SlowTerminatingService(i)};

    @BeforeAll
    static void beforeAll()
    {
        clusterInstrumentor1 = new ClusterInstrumentor(
//            BlockLogAdapterPoll.class, "ServiceProxy", "terminationPosition");
            BlockLogAdapterPoll.class, "ConsensusModuleAgent", "onTerminationPosition");
    }

    @AfterAll
    static void afterAll()
    {
        clusterInstrumentor1.reset();
        ThreadBlocker.BLOCKER_BY_THREAD.clear();
    }

    @Test
    @InterruptAfter(20)
    void shouldNotThrowErrorWhenThereIsASimultaneousShutdownAndElection() throws InterruptedException
    {
        final TestCluster cluster = aCluster().withStaticNodes(3)
            .withServiceSupplier(serviceSupplier)
            .start();
        systemTestWatcher.cluster(cluster);

        final TestNode firstLeader = cluster.awaitLeader();

        assertEquals(Cluster.Role.LEADER, firstLeader.role());

        final int followerAIndex = (firstLeader.index() + 1) % 3;
        final int followerBIndex = (firstLeader.index() + 2) % 3;

        final ThreadBlocker followerABlocker =
            ThreadBlocker.withThreadNameMatching(".*consensus-module_0_" + followerAIndex + ".*");
        final CountDownLatch terminatingOnA = followerABlocker.block();

        final ThreadBlocker followerBBlocker =
            ThreadBlocker.withThreadNameMatching(".*consensus-module_0_" + followerBIndex + ".*");
        final CountDownLatch terminatingOnB = followerBBlocker.block();

        beforeTerminateAckLatch[firstLeader.index()].countDown();
//        cluster.abortCluster(firstLeader);
        cluster.shutdownCluster(firstLeader);

        terminatingOnA.await();
        terminatingOnB.await();

        firstLeader.gracefulClose();

        Thread.sleep(100);

        followerABlocker.release();
        followerBBlocker.release();


//        cluster.startStaticNode(firstLeader.index(), false);

        final TestNode secondLeader = cluster.awaitLeader();
        assertNotEquals(firstLeader.index(), secondLeader.index());

        for (int i = 0; i < beforeTerminateAckLatch.length; i++)
        {
            if (i != firstLeader.index() && i != secondLeader.index())
            {
                signalTerminateLatch[i].await();
                beforeTerminateAckLatch[i].countDown();
            }
        }

        Thread.sleep(1000);

        cluster.stopAllNodes();
    }

    private static class BlockLogAdapterPoll
    {
        @Advice.OnMethodEnter
        static void terminationPosition()
        {
            final Thread thread = Thread.currentThread();
            ThreadBlocker.forThread(thread).onEnter();
        }

    }

    public static class ThreadBlocker
    {
        public static final ConcurrentHashMap<Thread, ThreadBlocker> BLOCKER_BY_THREAD =
            new ConcurrentHashMap<>();

        private CountDownLatch signalLatch;
        private CountDownLatch awaitLatch;

        public static ThreadBlocker forThread(final Thread thread)
        {
            return ThreadBlocker.BLOCKER_BY_THREAD.computeIfAbsent(thread, ignored -> new ThreadBlocker());
        }

        public static ThreadBlocker withThreadNameMatching(final String pattern)
        {
            return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().matches(pattern))
                .map(ThreadBlocker::forThread)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                    "No thread found matching: /" + pattern + "/ " +
                        "ThreadBlocker.FOLLOWER_BY_THREAD=" + BLOCKER_BY_THREAD));
        }

        public void onEnter()
        {
            if (signalLatch != null)
            {
                signalLatch.countDown();
            }

            if (awaitLatch != null)
            {
                try
                {
                    awaitLatch.await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        void waitUntilHit()
        {
            signalLatch = new CountDownLatch(1);
            try
            {
                signalLatch.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

        CountDownLatch block()
        {
            assertNull(signalLatch);
            assertNull(awaitLatch);
            signalLatch = new CountDownLatch(1);
            awaitLatch = new CountDownLatch(1);
            return signalLatch;
        }

        void release()
        {
            assertNotNull(signalLatch);
            assertNotNull(awaitLatch);
            awaitLatch.countDown();
            signalLatch = null;
            awaitLatch = null;
        }
    }

    private class SlowTerminatingService extends TestNode.TestService
    {
        private final int index;

        public SlowTerminatingService(final int index)
        {
            this.index = index;
            index(index);
        }

        @Override
        public void onTerminate(final Cluster cluster)
        {
            super.onTerminate(cluster);
            try
            {
                signalTerminateLatch[index].countDown();
                beforeTerminateAckLatch[index].await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
