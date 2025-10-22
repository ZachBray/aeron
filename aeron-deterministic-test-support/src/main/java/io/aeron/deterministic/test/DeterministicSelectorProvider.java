package io.aeron.deterministic.test;

import java.net.ProtocolFamily;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;

public final class DeterministicSelectorProvider extends SelectorProvider
{
    private static final DeterministicSelectorProvider INSTANCE = new DeterministicSelectorProvider();

    private DeterministicSelectorProvider()
    {
    }

    public static DeterministicSelectorProvider instance()
    {
        return INSTANCE;
    }

    public DatagramChannel openDatagramChannel()
    {
        return DeterministicNetwork.instance().openDatagramChannel(this);
    }

    public DatagramChannel openDatagramChannel(final ProtocolFamily family)
    {
        return DeterministicNetwork.instance().openDatagramChannel(this);
    }

    public Pipe openPipe()
    {
        throw new UnsupportedOperationException("openPipe not supported in deterministic tests");
    }

    public AbstractSelector openSelector()
    {
        return DeterministicNetwork.instance().openSelector(this);
    }

    public ServerSocketChannel openServerSocketChannel()
    {
        throw new UnsupportedOperationException("openServerSocketChannel not supported in deterministic tests");
    }

    public SocketChannel openSocketChannel()
    {
        throw new UnsupportedOperationException("openSocketChannel not supported in deterministic tests");
    }
}
