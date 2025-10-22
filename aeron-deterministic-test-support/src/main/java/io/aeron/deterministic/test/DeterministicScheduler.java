package io.aeron.deterministic.test;

import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.IdleStrategy;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

public final class DeterministicScheduler
{
    private static final Comparator<ExecutionLessee> LESSEE_ORDER =
        Comparator.comparing(ExecutionLessee::agentId);
    private final LeaseTransferIdleStrategy idleStrategy = new LeaseTransferIdleStrategy();
    private final Map<Thread, ExecutionLessee> lesseeByThread = new HashMap<>();
    private final AtomicBoolean terminated = new AtomicBoolean();
    private final ExecutionStrategy executionStrategy;
    private final long timeout;
    private final TimeUnit timeUnit;
    private final ThreadFactory threadFactory;
    private ExecutionLessee[] lessees = new ExecutionLessee[0];
    private BooleanSupplier terminationCondition;
    private CountDownLatch terminationLatch;

    public DeterministicScheduler(
        final ExecutionStrategy executionStrategy,
        final long timeout,
        final TimeUnit timeUnit,
        final ThreadFactory threadFactory)
    {
        this.executionStrategy = executionStrategy;
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.threadFactory = threadFactory;
    }

    public static boolean runSystemUntil(
        final ExecutionStrategy executionStrategy,
        final long timeout,
        final TimeUnit timeUnit,
        final BooleanSupplier terminationCondition,
        final AgentInvoker... agents
    )
    {
        final DeterministicScheduler scheduler = new DeterministicScheduler(
            executionStrategy,
            timeout,
            timeUnit,
            Thread.ofVirtual().factory()
        );

        for (final AgentInvoker agent : agents)
        {
            scheduler.schedule(agent);
        }

        return scheduler.runSystemUntil(terminationCondition);
    }

    public boolean runSystemUntil(final BooleanSupplier condition)
    {
        this.terminationCondition = condition;
        this.terminationLatch = new CountDownLatch(lessees.length);
        try
        {
            transferLease();
            return terminationLatch.await(timeout, timeUnit);
        }
        catch (Exception exception)
        {
            LangUtil.rethrowUnchecked(exception);
            return false;
        }
        finally
        {
            lesseeByThread.forEach((thread, lessee) ->
            {
                try
                {
                    thread.join();
                }
                catch (InterruptedException ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
            });
        }
    }

    public void schedule(final AgentInvoker invoker)
    {
        String agentId = invoker.agent().roleName();

        for (int i = 2; i < 100; i++)
        {
            boolean hasCollision = false;
            for (final ExecutionLessee executionLessee : lessees)
            {
                if (executionLessee.agentId().equals(agentId))
                {
                    hasCollision = true;
                    break;
                }
            }

            if (hasCollision)
            {
                agentId = invoker.agent().roleName() + "-" + i;
            }
            else
            {
                break;
            }
        }

        final ExecutionLessee lessee = new ExecutionLessee(agentId);

        lessees = ArrayUtil.add(lessees, lessee);
        Arrays.sort(lessees, LESSEE_ORDER);

        final Thread thread = threadFactory.newThread(() ->
        {
            try
            {
                if (!invoker.isStarted())
                {
                    lessee.acquireLease();
                    try
                    {
                        invoker.start();
                    }
                    finally
                    {
                        lessee.releaseLease();
                    }
                }

                while (invoker.isRunning() && !terminated.get())
                {
                    lessee.acquireLease();
                    try
                    {
                        invoker.invoke();
                    }
                    finally
                    {
                        lessee.releaseLease();
                    }
                }

//                if (!invoker.isClosed())
//                {
//                    lessee.acquireLease();
//                    try
//                    {
//                        invoker.close();
//                    }
//                    finally
//                    {
//                        lessee.releaseLease();
//                    }
//                }
            }
            finally
            {
                lessee.acquireLease();
                try
                {
                    lessees = ArrayUtil.remove(lessees, lessee);
                }
                finally
                {
                    lessee.releaseLease();
                }
                terminationLatch.countDown();
            }
        });


        thread.setName(lessee.agentId());
        thread.start();

        lesseeByThread.put(thread, lessee);
    }

    private void transferLease()
    {
        for (final ExecutionLessee lessee : lessees)
        {
            if (lessee.hasLease())
            {
                throw new IllegalStateException("Expected no component threads to be running.");
            }
        }

        if (terminationCondition.getAsBoolean())
        {
            terminated.set(true);
        }
        else if (lessees.length == 0)
        {
            throw new IllegalStateException("No lessees available to transfer lease.");
        }

        if (lessees.length > 0)
        {
            final ExecutionLessee lessee = executionStrategy.choose(lessees);
            lessee.grantLease();
        }
    }

    public IdleStrategy idleStrategy()
    {
        return idleStrategy;
    }

    public interface ExecutionStrategy
    {
        ExecutionLessee choose(ExecutionLessee[] lessees);
    }

    public static final class RandomExecutionStrategy implements ExecutionStrategy
    {
        private final Random random = new Random();

        public RandomExecutionStrategy(final long seed)
        {
            random.setSeed(seed);
        }

        public ExecutionLessee choose(final ExecutionLessee[] lessees)
        {
            return lessees[random.nextInt(lessees.length)];
        }
    }

    private class LeaseTransferIdleStrategy implements IdleStrategy
    {
        public void idle(final int workDone)
        {
            idle();
        }

        public void idle()
        {
            final ExecutionLessee executionLessee = lesseeByThread.get(Thread.currentThread());
            executionLessee.releaseLease();
            executionLessee.acquireLease();
        }

        public void reset()
        {
            // No-op
        }
    }

    public final class ExecutionLessee
    {
        private final String agentId;
        private final Object gate = new Object();
        private State state = State.WAITING_FOR_LEASE;

        public ExecutionLessee(final String agentId)
        {
            this.agentId = agentId;
        }

        public String agentId()
        {
            return agentId;
        }

        void grantLease()
        {
            synchronized (gate)
            {
                if (state != State.WAITING_FOR_LEASE)
                {
                    throw new IllegalStateException("Lease was already granted.");
                }

                state = State.HAS_LEASE;

                gate.notify();
            }
        }

        void acquireLease()
        {
            try
            {
                synchronized (gate)
                {
                    while (state != State.HAS_LEASE)
                    {
                        gate.wait();
                    }
                }
            }
            catch (InterruptedException exception)
            {
                LangUtil.rethrowUnchecked(exception);
            }
        }

        void releaseLease()
        {
            synchronized (gate)
            {
                if (state != State.HAS_LEASE)
                {
                    throw new IllegalStateException("Lease was not acquired before release.");
                }

                state = State.WAITING_FOR_LEASE;
            }

            transferLease();
        }

        public boolean hasLease()
        {
            synchronized (gate)
            {
                return state == State.HAS_LEASE;
            }
        }

        enum State
        {
            WAITING_FOR_LEASE,
            HAS_LEASE
        }
    }
}
