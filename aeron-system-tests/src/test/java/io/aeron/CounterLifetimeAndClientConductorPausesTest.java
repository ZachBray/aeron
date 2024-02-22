package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(InterruptingTestCallback.class)
public class CounterLifetimeAndClientConductorPausesTest
{
    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    @Test
    void duplicateMessageDueTo2SecondClientConductorPauseAndCounterReuse() throws InterruptedException
    {
        final MutableInteger messageCount = new MutableInteger();
        final FragmentHandler messageCounter = (buffer, offset, length, header) -> messageCount.value++;
        final FragmentAssembler fragmentAssembler = new FragmentAssembler(messageCounter);
        final UnsafeBuffer messageBuffer = new UnsafeBuffer(new byte[8]);
        messageBuffer.putLong(0, 42);

        try (final TestMediaDriver driver = launchDriver();
             final Aeron clientA = launchClient();
             final Aeron clientB = launchClient();
        )
        {
            driver.sharedAgentInvoker().start();
            clientA.conductorAgentInvoker().start();
            clientB.conductorAgentInvoker().start();

            final long pubRegistrationId = clientA.asyncAddExclusivePublication("aeron:ipc", 42);
            final long subRegistrationId = clientB.asyncAddSubscription("aeron:ipc", 42);

            ExclusivePublication publication = null;
            Subscription subscription = null;
            Image image = null;

            while (publication == null || subscription == null || image == null)
            {
                invokeInvokers(
                    driver.sharedAgentInvoker(),
                    clientA.conductorAgentInvoker(),
                    clientB.conductorAgentInvoker()
                );

                if (publication == null)
                {
                    publication = clientA.getExclusivePublication(pubRegistrationId);
                }

                if (subscription == null)
                {
                    subscription = clientB.getSubscription(subRegistrationId);
                }

                if (subscription != null && subscription.imageCount() > 0)
                {
                    image = subscription.imageAtIndex(0);
                }
            }

            while (publication.offer(messageBuffer, 0, messageBuffer.capacity()) < 0)
            {
                invokeInvokers(
                    driver.sharedAgentInvoker(),
                    clientA.conductorAgentInvoker(),
                    clientB.conductorAgentInvoker()
                );
            }

            final int subPosCounterId = image.subscriberPositionId();

            while (image.position() < publication.position())
            {
                final int fragmentsConsumed = image.poll(fragmentAssembler, 1);
                assert fragmentsConsumed != 0;

                invokeInvokers(
                    driver.sharedAgentInvoker(),
                    clientA.conductorAgentInvoker(),
                    clientB.conductorAgentInvoker()
                );
            }

            publication.close();

            long dueTimeMs = System.currentTimeMillis() + 2_000;
            while (System.currentTimeMillis() < dueTimeMs)
            {
                invokeInvokers(
                    driver.sharedAgentInvoker(),
                    clientA.conductorAgentInvoker()
                    // clientB pauses for ~ 2 seconds
                );
            }

            // Image looks alive
            assertFalse(image.isClosed());

            // Counter is closed
            final int counterState = clientB.countersReader().getCounterState(subPosCounterId);
            assertEquals(CountersReader.RECORD_RECLAIMED, counterState);

            // No async counter API so we invoke the driver in another thread:
            final AtomicBoolean creatingCounterReuseCondition = new AtomicBoolean(true);
            final Thread driverThread = new Thread(() ->
            {
                while (creatingCounterReuseCondition.get())
                {
                    final int workDone = driver.sharedAgentInvoker().invoke();
                    if (workDone == 0)
                    {
                        Tests.yield();
                    }
                }
            });
            driverThread.start();

            // Simulate counter reuse, e.g., adding another publication
            final List<Counter> counters = new ArrayList<>();
            while (creatingCounterReuseCondition.get())
            {
                final Counter counter = clientA.addCounter(424242, "test counter");
                counters.add(counter);
                if (counter.id() == subPosCounterId)
                {
                    creatingCounterReuseCondition.set(false);
                }
            }

            driverThread.join();

            final Counter reusedCounter = counters.remove(counters.size() - 1);
            reusedCounter.setOrdered(0);
            CloseHelper.closeAll(counters);

            image.poll(fragmentAssembler, 1);

            // Duplicate message!
            assertEquals(2, messageCount.get());

            invokeInvokers(
                driver.sharedAgentInvoker(),
                clientA.conductorAgentInvoker(),
                clientB.conductorAgentInvoker()
            );
        }
    }

    private void invokeInvokers(final AgentInvoker... invokers)
    {
        for (final AgentInvoker invoker : invokers)
        {
            invoker.invoke();
        }
    }

    private TestMediaDriver launchDriver()
    {
        return TestMediaDriver.launch(
            new MediaDriver.Context()
                .errorHandler(Tests::onError)
                .threadingMode(ThreadingMode.INVOKER)
                .counterValuesBufferLength(1024 * 1024)
                .ipcTermBufferLength(64 * 1024),
            testWatcher);
    }

    private static Aeron launchClient()
    {
        return Aeron.connect(new Aeron.Context()
            .useConductorAgentInvoker(true)
            .errorHandler(Tests::onError));
    }
}
