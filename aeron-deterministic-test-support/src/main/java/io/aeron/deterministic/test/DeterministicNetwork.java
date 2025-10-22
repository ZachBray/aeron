package io.aeron.deterministic.test;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.AbstractSelectionKey;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.LongFunction;

import static java.net.StandardSocketOptions.IP_MULTICAST_TTL;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;
import static org.agrona.BufferUtil.allocateDirectAligned;

public final class DeterministicNetwork implements Agent
{
    private static final DeterministicNetwork INSTANCE = new DeterministicNetwork();
    private static final int SOCKET_BUFFER_SIZE = 131072;
    private static final int WILDCARD_ADDRESS = 0;
    private static final int WILDCARD_PORT = 0;
    private static final long WILDCARD_SOCKET_ADDRESS = packSocketAddress(WILDCARD_ADDRESS, WILDCARD_PORT);
    private static final int WILDCARD_REPLACEMENT_PORT = 65535;
    private static final IntFunction<InetAddress> INET_ADDRESS_FACTORY = DeterministicNetwork::createInetAddress;
    private final LongFunction<InetSocketAddress> inetSocketAddressFactory = this::createInetSocketAddress;
    private final List<DeterministicDatagramChannel> channels = new ArrayList<>();
    private final Long2ObjectHashMap<DeterministicDatagramChannel> channelsByBoundSocketAddress =
        new Long2ObjectHashMap<>();
    private final Int2ObjectHashMap<InetAddress> addresses = new Int2ObjectHashMap<>();
    private final Long2ObjectHashMap<InetSocketAddress> socketAddresses = new Long2ObjectHashMap<>();
    private final DatagramFlyweight datagramFlyweight = new DatagramFlyweight();
    private final DatagramReader datagramReader = new DatagramReader();
    private final DatagramSender datagramSender = new DatagramSender();
    private int nextWildcardReplacementAddress = 10 << 24;

    public static DeterministicNetwork instance()
    {
        return INSTANCE;
    }

    public DatagramChannel openDatagramChannel(final DeterministicSelectorProvider provider)
    {
        return new DeterministicDatagramChannel(provider);
    }

    public AbstractSelector openSelector(final DeterministicSelectorProvider provider)
    {
        return new DeterministicSelector(provider);
    }

    public int doWork()
    {
        int workDone = 0;
        for (DeterministicDatagramChannel channel : channels)
        {
            workDone += channel.doWork();
        }

        return workDone;
    }

    public String roleName()
    {
        return "DeterministicNetwork";
    }

    private InetSocketAddress createInetSocketAddress(long packedSocketAddress)
    {
        final int address = unpackAddress(packedSocketAddress);
        final int port = unpackPort(packedSocketAddress);
        final InetAddress inetAddress = addresses.computeIfAbsent(address, INET_ADDRESS_FACTORY);
        return new InetSocketAddress(inetAddress, port);
    }

    private static long packSocketAddress(final int address, final int port)
    {
        // Little-endian address first
        return (((long)port) << 32) | address;
    }

    private static int unpackPort(final long packedSocketAddress)
    {
        return (int)(packedSocketAddress >> 32);
    }

    private static int unpackAddress(final long packedSocketAddress)
    {
        return (int)(packedSocketAddress & 0xFFFFFFFFL);
    }

    private InetSocketAddress getInetSocketAddress(final long packedSocketAddress)
    {
        return socketAddresses.computeIfAbsent(packedSocketAddress, inetSocketAddressFactory);
    }

    private static long getPackedSocketAddress(final InetSocketAddress socketAddress)
    {
        final byte[] address = socketAddress.getAddress().getAddress();
        assert address.length == 4 : "Only IPv4 addresses are supported";
        long packedSocketAddress = ((long)socketAddress.getPort()) << 32;
        packedSocketAddress |= (address[0] & 0xFFL) << 24;
        packedSocketAddress |= (address[1] & 0xFFL) << 16;
        packedSocketAddress |= (address[2] & 0xFFL) << 8;
        packedSocketAddress |= (address[3] & 0xFFL);
        return packedSocketAddress;
    }

    private static InetAddress createInetAddress(int addressId)
    {
        try
        {
            return InetAddress.getByAddress(
                new byte[]{
                    (byte)(addressId >> 24),
                    (byte)(addressId >> 16),
                    (byte)(addressId >> 8),
                    (byte)addressId
                }
            );
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    private final class DeterministicDatagramChannel extends DatagramChannel
    {
        public static final int DATA_MSG_TYPE_ID = 1;
        // TODO use a single-threaded buffer without direct memory
        private final RingBuffer sendBuffer = new OneToOneRingBuffer(new UnsafeBuffer(allocateDirectAligned(
            SOCKET_BUFFER_SIZE + RingBufferDescriptor.TRAILER_LENGTH, BitUtil.CACHE_LINE_LENGTH)));
        private final RingBuffer receiveBuffer = new OneToOneRingBuffer(new UnsafeBuffer(allocateDirectAligned(
            SOCKET_BUFFER_SIZE + RingBufferDescriptor.TRAILER_LENGTH, BitUtil.CACHE_LINE_LENGTH)));
        private InetSocketAddress boundAddress;
        private long packedBoundAddress = WILDCARD_SOCKET_ADDRESS;
        private boolean isClosed;
        private InetSocketAddress remoteAddress;
        private long packedRemoteAddress = WILDCARD_SOCKET_ADDRESS;

        /**
         * Initializes a new instance of this class.
         *
         * @param provider The provider that created this channel
         */
        DeterministicDatagramChannel(final SelectorProvider provider)
        {
            super(provider);
            channels.add(this);
        }

        public DatagramChannel bind(final SocketAddress local) throws IOException
        {
            if (isClosed)
            {
                throw new ClosedChannelException();
            }

            if (null != boundAddress)
            {
                throw new AlreadyBoundException();
            }

            if (null == local)
            {
                packedBoundAddress = WILDCARD_SOCKET_ADDRESS;
                boundAddress = getInetSocketAddress(packedBoundAddress);
            }
            else if (local instanceof InetSocketAddress inetSocketAddress)
            {
                packedBoundAddress = getPackedSocketAddress(inetSocketAddress);
                boundAddress = inetSocketAddress;
            }
            else
            {
                throw new IllegalArgumentException("Unsupported address type: " + local.getClass());
            }

            if (packedBoundAddress == WILDCARD_SOCKET_ADDRESS)
            {
                // TODO: model nodes and interfaces rather than assigning a random address
                packedBoundAddress = packSocketAddress(nextWildcardReplacementAddress++, WILDCARD_REPLACEMENT_PORT);
                boundAddress = getInetSocketAddress(packedBoundAddress);
            }

            final DeterministicDatagramChannel existing = channelsByBoundSocketAddress.putIfAbsent(
                packedBoundAddress,
                this
            );

            if (null != existing)
            {
                throw new IllegalStateException(
                    "Channel already bound to address: " + existing.getLocalAddress());
            }

            return this;
        }

        public <T> DatagramChannel setOption(final SocketOption<T> name, final T value)
        {
            return this;
        }

        @SuppressWarnings("unchecked")
        public <T> T getOption(final SocketOption<T> name)
        {
            if (name == SO_RCVBUF || name == SO_SNDBUF)
            {
                return (T)Integer.valueOf(SOCKET_BUFFER_SIZE);
            }
            if (name == IP_MULTICAST_TTL)
            {
                return (T)Integer.valueOf(8);
            }
            throw new UnsupportedOperationException("Unsupported option: " + name);
        }

        public Set<SocketOption<?>> supportedOptions()
        {
            return Set.of(SO_SNDBUF, SO_RCVBUF, IP_MULTICAST_TTL);
        }

        public DatagramSocket socket()
        {
            throw new UnsupportedOperationException("socket not supported in deterministic tests");
        }

        public boolean isConnected()
        {
            return null != remoteAddress;
        }

        public DatagramChannel connect(final SocketAddress remote) throws IOException
        {
            if (isClosed)
            {
                throw new ClosedChannelException();
            }

            if (null != remoteAddress)
            {
                throw new AlreadyConnectedException();
            }

            if (null == boundAddress)
            {
                //noinspection resource
                bind(null);
            }

            if (remote instanceof InetSocketAddress inetSocketAddress)
            {
                remoteAddress = inetSocketAddress;
                packedRemoteAddress = getPackedSocketAddress(inetSocketAddress);
            }
            else
            {
                throw new IllegalArgumentException("Unsupported address type: " + remote.getClass());
            }

            return this;
        }

        public DatagramChannel disconnect()
        {
            if (isClosed)
            {
                return this;
            }

            remoteAddress = null;
            packedRemoteAddress = WILDCARD_SOCKET_ADDRESS;

            return this;
        }

        public SocketAddress getRemoteAddress()
        {
            return remoteAddress;
        }

        public SocketAddress receive(final ByteBuffer dst) throws ClosedChannelException
        {
            if (isClosed)
            {
                throw new ClosedChannelException();
            }

            receiveBuffer.read(datagramReader.reset(dst, packedRemoteAddress), 1);
            if (datagramReader.hasCopiedData)
            {
                if (datagramReader.receivedPackedSrcSocketAddress == packedRemoteAddress)
                {
                    return remoteAddress;
                }

                return getInetSocketAddress(datagramReader.receivedPackedSrcSocketAddress);
            }

            return null;
        }

        public int send(final ByteBuffer src, final SocketAddress target) throws IOException
        {
            if (isClosed)
            {
                throw new ClosedChannelException();
            }

            if (null != remoteAddress && !remoteAddress.equals(target))
            {
                throw new AlreadyConnectedException();
            }

            return write0(src, target);
        }

        public int read(final ByteBuffer dst)
        {
            throw new UnsupportedOperationException(
                "read(ByteBuffer) not supported in deterministic tests");
        }

        public long read(final ByteBuffer[] dsts, final int offset, final int length)
        {
            throw new UnsupportedOperationException(
                "read(ByteBuffer[], int, int) not supported in deterministic tests");
        }

        public int write(final ByteBuffer src) throws ClosedChannelException
        {
            if (isClosed)
            {
                throw new ClosedChannelException();
            }

            if (null == remoteAddress)
            {
                throw new NotYetConnectedException();
            }

            return write0(src, remoteAddress);
        }

        public long write(final ByteBuffer[] srcs, final int offset, final int length)
        {
            throw new UnsupportedOperationException(
                "write(ByteBuffer[], int, int) not supported in deterministic tests");
        }

        public int write0(final ByteBuffer data, final SocketAddress target)
        {
            if (target instanceof InetSocketAddress inetSocketAddress)
            {
                return write1(data, inetSocketAddress);
            }
            else
            {
                throw new IllegalArgumentException("Unsupported address type: " + target.getClass());
            }
        }

        private int write1(final ByteBuffer data, final InetSocketAddress target)
        {
            final int dataLength = data.remaining();
            final int length = DatagramFlyweight.HEADER_LENGTH + dataLength;
            final int index = sendBuffer.tryClaim(DATA_MSG_TYPE_ID, length);
            if (RingBuffer.INSUFFICIENT_CAPACITY == index)
            {
                return 0;
            }

            datagramFlyweight.wrap(sendBuffer.buffer(), index, length);
            datagramFlyweight.encode(
                packedBoundAddress,
                target,
                data
            );
            sendBuffer.commit(index);

            return dataLength;
        }

        public SocketAddress getLocalAddress()
        {
            return boundAddress;
        }

        public MembershipKey join(final InetAddress group, final NetworkInterface interf)
        {
            throw new UnsupportedOperationException(
                "Multicast not supported in deterministic tests");
        }

        public MembershipKey join(final InetAddress group, final NetworkInterface interf, final InetAddress source)
        {
            throw new UnsupportedOperationException(
                "Multicast with source not supported in deterministic tests");
        }

        protected void implCloseSelectableChannel()
        {
            isClosed = true;
            channels.remove(this);
        }

        protected void implConfigureBlocking(final boolean block)
        {
        }

        public int doWork()
        {
            return sendBuffer.read(datagramSender, 1);
        }
    }

    private final class DatagramSender implements MessageHandler
    {
        public void onMessage(
            final int msgTypeId,
            final MutableDirectBuffer buffer,
            final int index,
            final int length)
        {
            if (msgTypeId != DeterministicDatagramChannel.DATA_MSG_TYPE_ID)
            {
                return;
            }

            datagramFlyweight.wrap(buffer, index, length);

            final long dstAddress = datagramFlyweight.decodePackedDstSocketAddress();
            final int dataLength = datagramFlyweight.decodeDataLength();

            final DeterministicDatagramChannel dstChannel = channelsByBoundSocketAddress.get(dstAddress);
            if (dstChannel == null)
            {
                return;
            }

            final RingBuffer receiveBuffer = dstChannel.receiveBuffer;
            final int receiveBufferIndex = receiveBuffer.tryClaim(
                DeterministicDatagramChannel.DATA_MSG_TYPE_ID, DatagramFlyweight.HEADER_LENGTH + dataLength);
            if (receiveBufferIndex > 0)
            {
                datagramFlyweight.copyDatagramTo(receiveBuffer.buffer(), receiveBufferIndex);
                receiveBuffer.commit(receiveBufferIndex);
            }
        }
    }

    private final class DatagramReader implements MessageHandler
    {
        private ByteBuffer destinationBuffer;
        private boolean hasCopiedData;
        private int expectedSrcAddress;
        private int expectedSrcPort;
        private long receivedPackedSrcSocketAddress;

        public DatagramReader reset(
            final ByteBuffer destinationBuffer,
            final long expectedPackedSrcSocketAddress)
        {
            this.destinationBuffer = destinationBuffer;
            this.expectedSrcAddress = unpackAddress(expectedPackedSrcSocketAddress);
            this.expectedSrcPort = unpackPort(expectedPackedSrcSocketAddress);
            this.hasCopiedData = false;
            return this;
        }

        public void onMessage(
            final int msgTypeId,
            final MutableDirectBuffer buffer,
            final int index,
            final int length)
        {
            if (msgTypeId != DeterministicDatagramChannel.DATA_MSG_TYPE_ID)
            {
                return;
            }

            datagramFlyweight.wrap(buffer, index, length);

            final int srcAddress = datagramFlyweight.decodeSrcAddress();
            final int srcPort = datagramFlyweight.decodeSrcPort();
            receivedPackedSrcSocketAddress = datagramFlyweight.decodePackedSrcSocketAddress();

            final boolean matchesAddress = expectedSrcAddress == WILDCARD_ADDRESS ||
                expectedSrcAddress == srcAddress;

            final boolean matchesPort = expectedSrcPort == WILDCARD_PORT ||
                expectedSrcPort == srcPort;

            if (matchesAddress && matchesPort)
            {
                datagramFlyweight.copyDataTo(destinationBuffer);
                hasCopiedData = true;
            }
        }
    }

    private final static class DatagramFlyweight
    {
        private static final int SRC_IPV4_OFFSET = 0;
        private static final int SRC_PORT_OFFSET = 4;
        private static final int DST_IPV4_OFFSET = 8;
        @SuppressWarnings("unused")
        private static final int DST_PORT_OFFSET = 12;
        private static final int DATA_LENGTH_OFFSET = 16;
        private static final int DATA_OFFSET = 20;
        private static final int HEADER_LENGTH = DATA_OFFSET;
        private final UnsafeBuffer buffer = new UnsafeBuffer();

        public void wrap(final DirectBuffer buffer, final int offset, final int length)
        {
            this.buffer.wrap(buffer, offset, length);
        }

        public void encode(
            final long packedSrcSocketAddress,
            final InetSocketAddress dst,
            final ByteBuffer data)
        {
            buffer.putLong(SRC_IPV4_OFFSET, packedSrcSocketAddress);
            buffer.putLong(DST_IPV4_OFFSET, getPackedSocketAddress(dst));
            buffer.putInt(DATA_LENGTH_OFFSET, data.remaining());
            buffer.putBytes(DATA_OFFSET, data, data.remaining());
        }

        public int decodeSrcAddress()
        {
            return buffer.getInt(SRC_IPV4_OFFSET);
        }

        public int decodeSrcPort()
        {
            return buffer.getInt(SRC_PORT_OFFSET);
        }

        public long decodePackedSrcSocketAddress()
        {
            return buffer.getLong(SRC_IPV4_OFFSET);
        }

        public long decodePackedDstSocketAddress()
        {
            return buffer.getLong(DST_IPV4_OFFSET);
        }

        public int decodeDataLength()
        {
            return buffer.getInt(DATA_LENGTH_OFFSET);
        }

        public void copyDataTo(final ByteBuffer destination)
        {
            final int dataLength = buffer.getInt(DATA_LENGTH_OFFSET);
            if (dataLength > 0)
            {
                buffer.getBytes(DATA_OFFSET, destination, Math.min(dataLength, destination.remaining()));
            }
        }

        public void copyDatagramTo(final MutableDirectBuffer dstBuffer, final int receiveBufferIndex)
        {
            dstBuffer.putBytes(receiveBufferIndex, buffer, 0, buffer.capacity());
        }
    }

    private static final class DeterministicSelector extends AbstractSelector
    {
        private final Set<SelectionKey> keys = new HashSet<>();

        public DeterministicSelector(final DeterministicSelectorProvider provider)
        {
            super(provider);
        }

        protected void implCloseSelector()
        {
        }

        protected SelectionKey register(final AbstractSelectableChannel ch, final int ops, final Object att)
        {
            return new DeterministicSelectionKey(this, ch, ops, att);
        }

        public Set<SelectionKey> keys()
        {
            return keys;
        }

        public Set<SelectionKey> selectedKeys()
        {
            return keys;
        }

        public int selectNow()
        {
            return keys.size();
        }

        public int select(final long timeout)
        {
            return keys.size();
        }

        public int select()
        {
            return keys.size();
        }

        public Selector wakeup()
        {
            return this;
        }

        private static final class DeterministicSelectionKey extends AbstractSelectionKey
        {
            private final AbstractSelector selector;
            private final AbstractSelectableChannel channel;
            private final int ops;

            public DeterministicSelectionKey(
                final AbstractSelector selector,
                final AbstractSelectableChannel channel,
                final int ops,
                final Object attachment)
            {
                this.selector = selector;
                this.channel = channel;
                this.ops = ops;
                attach(attachment);
            }

            public SelectableChannel channel()
            {
                return channel;
            }

            public Selector selector()
            {
                return selector;
            }

            @SuppressWarnings("MagicConstant")
            public int interestOps()
            {
                return ops;
            }

            public SelectionKey interestOps(final int ops)
            {
                throw new UnsupportedOperationException(
                    "Changing interestOps is not supported in deterministic tests");
            }

            @SuppressWarnings("MagicConstant")
            public int readyOps()
            {
                return ops;
            }
        }
    }
}
