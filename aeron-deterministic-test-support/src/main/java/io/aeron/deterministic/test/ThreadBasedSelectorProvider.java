package io.aeron.deterministic.test;

import org.agrona.LangUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;

import java.io.IOException;
import java.net.ProtocolFamily;
import java.nio.channels.Channel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.ThreadFactory;

import static java.util.Objects.requireNonNull;

public class ThreadBasedSelectorProvider extends SelectorProvider
{
    private static final SelectorProvider DEFAULT_PROVIDER = requireNonNull(loadDefaultProvider());
    private static final Long2ObjectHashMap<SelectorProvider> THREAD_SELECTOR_PROVIDERS = new Long2ObjectHashMap<>();

    private static SelectorProvider loadDefaultProvider() {
        try {
            return (SelectorProvider)DeterministicSelectorProvider.class.getClassLoader()
                .loadClass("sun.nio.ch.DefaultSelectorProvider")
                .getDeclaredMethod("get")
                .invoke(null);
        } catch (Exception exception) {
            LangUtil.rethrowUnchecked(exception);
            return null;
        }
    }

    public static SelectorRegion useSelectorOnThisThread(
        final SelectorProvider selectorProvider
    )
    {
        final long threadId = Thread.currentThread().threadId();

        synchronized (THREAD_SELECTOR_PROVIDERS)
        {
            THREAD_SELECTOR_PROVIDERS.put(threadId, selectorProvider);
        }

        return new SelectorRegion(threadId);
    }

    public static ThreadFactory useSelector(
        final ThreadFactory factory,
        final SelectorProvider selectorProvider)
    {
        return runnable ->
            factory.newThread(() ->
            {
                synchronized (THREAD_SELECTOR_PROVIDERS)
                {
                    THREAD_SELECTOR_PROVIDERS.put(Thread.currentThread().threadId(), selectorProvider);
                }

                try
                {
                    runnable.run();
                }
                finally
                {
                    synchronized (THREAD_SELECTOR_PROVIDERS)
                    {
                        THREAD_SELECTOR_PROVIDERS.remove(Thread.currentThread().threadId());
                    }
                }
            });
    }

    private SelectorProvider delegate() {
        synchronized (THREAD_SELECTOR_PROVIDERS)
        {
            return THREAD_SELECTOR_PROVIDERS.getOrDefault(
                Thread.currentThread().threadId(),
                DEFAULT_PROVIDER
            );
        }
    }

    public DatagramChannel openDatagramChannel() throws IOException
    {
        return delegate().openDatagramChannel();
    }

    public DatagramChannel openDatagramChannel(final ProtocolFamily family) throws IOException
    {
        return delegate().openDatagramChannel(family);
    }

    public Pipe openPipe() throws IOException
    {
        return delegate().openPipe();
    }

    public AbstractSelector openSelector() throws IOException
    {
        return delegate().openSelector();
    }

    public ServerSocketChannel openServerSocketChannel() throws IOException
    {
        return delegate().openServerSocketChannel();
    }

    public SocketChannel openSocketChannel() throws IOException
    {
        return delegate().openSocketChannel();
    }

    public Channel inheritedChannel() throws IOException
    {
        return delegate().inheritedChannel();
    }

    public SocketChannel openSocketChannel(final ProtocolFamily family) throws IOException
    {
        return delegate().openSocketChannel(family);
    }

    public ServerSocketChannel openServerSocketChannel(final ProtocolFamily family) throws IOException
    {
        return delegate().openServerSocketChannel(family);
    }

    public static final class SelectorRegion implements AutoCloseable
    {
        private final long threadId;

        private SelectorRegion(final long threadId)
        {
            this.threadId = threadId;
        }

        public void close()
        {
            synchronized (THREAD_SELECTOR_PROVIDERS)
            {
                THREAD_SELECTOR_PROVIDERS.remove(threadId);
            }
        }
    }
}
