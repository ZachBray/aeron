package io.aeron.deterministic.cluster.test;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.deterministic.test.DeterministicNetwork;
import io.aeron.deterministic.test.DeterministicScheduler;
import io.aeron.deterministic.test.DeterministicSelectorProvider;
import io.aeron.deterministic.test.ThreadBasedSelectorProvider;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Random;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterPropertyTest
{
    @Test
    @SuppressWarnings("try")
    void shouldTransferMessages()
    {
        final Deque<AutoCloseable> closeables = new ArrayDeque<>();
        final DeterministicScheduler.ExecutionStrategy executionStrategy =
            new DeterministicScheduler.RandomExecutionStrategy(new Random().nextLong());
        final DeterministicSelectorProvider selectorProvider = DeterministicSelectorProvider.instance();
        final ThreadFactory threadFactory = ThreadBasedSelectorProvider.useSelector(
            Thread.ofVirtual().factory(),
            selectorProvider
        );
        final DeterministicScheduler scheduler = new DeterministicScheduler(
            executionStrategy,
            30, TimeUnit.SECONDS,
            threadFactory);

        // TODO clocks

        try (ThreadBasedSelectorProvider.SelectorRegion ignored =
            ThreadBasedSelectorProvider.useSelectorOnThisThread(selectorProvider))
        {
            // TODO reset network after each test
            scheduler.schedule(new AgentInvoker(Throwable::printStackTrace, null, DeterministicNetwork.instance()));

            final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .errorHandler(Throwable::printStackTrace)
                .threadingMode(ThreadingMode.INVOKER);
            final MediaDriver driver = MediaDriver.launch(driverCtx);
            closeables.addFirst(driver);
            scheduler.schedule(requireNonNull(driver.sharedAgentInvoker()));

            final Aeron client1 = Aeron.connect(new Aeron.Context()
                .errorHandler(Throwable::printStackTrace)
                .idleStrategy(scheduler.idleStrategy())
                .awaitingIdleStrategy(scheduler.idleStrategy())
                .useConductorAgentInvoker(true));
            closeables.addFirst(client1);
            scheduler.schedule(requireNonNull(client1.conductorAgentInvoker()));

            final Aeron client2 = Aeron.connect(new Aeron.Context()
                .errorHandler(Throwable::printStackTrace)
                .idleStrategy(scheduler.idleStrategy())
                .awaitingIdleStrategy(scheduler.idleStrategy())
                .useConductorAgentInvoker(true));
            closeables.addFirst(client2);
            scheduler.schedule(requireNonNull(client2.conductorAgentInvoker()));

            final int expectedMessageCount = 10;

            final Agent publisher = new Agent()
            {
                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8]);
                Publication publication;
                int messageSentCount = 0;

                public int doWork()
                {
                    if (null == publication)
                    {
                        publication = client1.addExclusivePublication("aeron:udp?endpoint=localhost:20123", 1001);
                    }

                    if (!publication.isConnected())
                    {
                        return 0;
                    }

                    if (messageSentCount < expectedMessageCount)
                    {
                        buffer.putLong(0, messageSentCount);
                        final long result = publication.offer(buffer, 0, 8);
                        if (result == Publication.CLOSED)
                        {
                            throw new IllegalStateException("Publication closed");
                        }
                        else if (result == Publication.MAX_POSITION_EXCEEDED)
                        {
                            throw new IllegalStateException("Max position exceeded");
                        }
                        else if (result > 0)
                        {
                            ++messageSentCount;
                            return 1;
                        }
                    }

                    return 0;
                }

                public String roleName()
                {
                    return "Publisher";
                }
            };
            scheduler.schedule(new AgentInvoker(LangUtil::rethrowUnchecked, null, publisher));

            final MutableInteger receivedMessageCount = new MutableInteger(0);
            final FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) ->
            {
                final int expectedContent = receivedMessageCount.getAndIncrement();
                final long actualContent = buffer.getLong(offset);
                if (expectedContent != actualContent)
                {
                    throw new IllegalStateException("Expected " + expectedContent + " but got " + actualContent);
                }
            });
            final Agent subscriber = new Agent()
            {
                Subscription subscription;

                public int doWork()
                {
                    if (subscription == null)
                    {
                        subscription = client2.addSubscription("aeron:udp?endpoint=localhost:20123", 1001);
                    }

                    return subscription.poll(assembler, 1);
                }

                public String roleName()
                {
                    return "Subscriber";
                }
            };
            scheduler.schedule(new AgentInvoker(LangUtil::rethrowUnchecked, null, subscriber));

            assertTrue(scheduler.runSystemUntil(() -> receivedMessageCount.get() >= expectedMessageCount));
        }
        finally
        {
            CloseHelper.closeAll(closeables);
        }
    }

    @Test
    void shouldEchoMessages()
    {
        final Random random = new Random();
        int count = 1000;
        int terminatedCount = 0;
        for (int i = 0; i < count; i++)
        {
            final long testSeed = random.nextLong();
            System.out.println("seed=" + testSeed);
            final DeterministicScheduler.ExecutionStrategy randomExecutionStrategy =
                new DeterministicScheduler.RandomExecutionStrategy(testSeed);

            final AtomicBoolean clientIsDone = new AtomicBoolean();
            final Agent clientAgent = new Agent()
            {
                public int doWork()
                {
                    clientIsDone.set(true);
                    return 0;
                }

                public String roleName()
                {
                    return "TestClient";
                }
            };
            final AgentInvoker clientAgentInvoker = new AgentInvoker(LangUtil::rethrowUnchecked, null, clientAgent);

            if (DeterministicScheduler.runSystemUntil(
                randomExecutionStrategy,
                30,
                TimeUnit.SECONDS,
                clientIsDone::get,
                clientAgentInvoker
            ))
            {
                ++terminatedCount;
            }
        }

        System.out.println("terminatedCount=" + terminatedCount + ", count=" + count);
    }
}
