package org.tarantool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Basic reconnection strategy that changes addresses in a round-robin fashion.
 * To be used with {@link TarantoolClientImpl}.
 */
public class RoundRobinSocketProviderImpl extends BaseSocketChannelProvider implements RefreshableSocketProvider {

    private static final int UNSET_POSITION = -1;
    private static final int DEFAULT_RETRIES_PER_CONNECTION = 3;

    /**
     * Socket addresses pool.
     */
    private final List<InetSocketAddress> socketAddresses = new ArrayList<>();

    /**
     * Current position within {@link #socketAddresses} list.
     * <p>
     * It is {@link #UNSET_POSITION} when no addresses from
     * the {@link #socketAddresses} pool have been processed yet.
     * <p>
     * When this provider receives new addresses it tries
     * to look for a new position for the last used address or
     * sets the position to {@link #UNSET_POSITION} otherwise.
     *
     * @see #getLastObtainedAddress()
     * @see #refreshAddresses(Collection)
     */
    private AtomicInteger currentPosition = new AtomicInteger(UNSET_POSITION);

    /**
     * Address list lock for a thread-safe access to it
     * when a refresh operation occurs.
     *
     * @see RefreshableSocketProvider#refreshAddresses(Collection)
     */
    private ReadWriteLock addressListLock = new ReentrantReadWriteLock();

    /**
     * Constructs an instance.
     *
     * @param addresses optional array of addresses in a form of host[:port]
     *
     * @throws IllegalArgumentException if addresses aren't provided
     */
    public RoundRobinSocketProviderImpl(String... addresses) {
        this(Arrays.asList(addresses));
    }

    /**
     * Constructs an instance.
     *
     * @param addresses optional list of addresses in a form of host[:port]
     *
     * @throws IllegalArgumentException if addresses aren't provided
     */
    public RoundRobinSocketProviderImpl(List<String> addresses) {
        updateAddressList(addresses);
        setRetriesLimit(DEFAULT_RETRIES_PER_CONNECTION);
    }

    private void updateAddressList(Collection<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException("At least one address must be provided");
        }
        Lock writeLock = addressListLock.writeLock();
        writeLock.lock();
        try {
            InetSocketAddress lastAddress = getLastObtainedAddress();
            socketAddresses.clear();
            addresses.stream()
                .map(this::parseAddress)
                .collect(Collectors.toCollection(() -> socketAddresses));
            if (lastAddress != null) {
                int recoveredPosition = socketAddresses.indexOf(lastAddress);
                currentPosition.set(recoveredPosition);
            } else {
                currentPosition.set(UNSET_POSITION);
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Gets parsed and resolved internet addresses.
     *
     * @return socket addresses
     */
    public List<SocketAddress> getAddresses() {
        return readGuard(() -> Collections.unmodifiableList(this.socketAddresses));
    }

    /**
     * Gets last used address from the pool if it exists.
     *
     * @return last obtained address or <code>null</code>
     *     if {@link #currentPosition} has {@link #UNSET_POSITION} value
     */
    protected InetSocketAddress getLastObtainedAddress() {
        return readGuard(() -> {
            int index = currentPosition.get();
            return index != UNSET_POSITION ? socketAddresses.get(index) : null;
        });
    }

    /**
     * Tries to open a socket channel to a next instance
     * for the addresses list.
     *
     * There are {@link #getRetriesLimit()} attempts per
     * call to initiate a connection to the instance.
     *
     * @param retryNumber reconnection attempt number
     * @param lastError   reconnection reason
     *
     * @return opened socket channel
     *
     * @throws IOException            if any IO errors occur
     * @throws CommunicationException if retry number exceeds addresses size
     *
     * @see #setRetriesLimit(int)
     * @see #getAddresses()
     */
    @Override
    protected SocketChannel makeAttempt(int retryNumber, Throwable lastError) throws IOException {
        if (retryNumber > getAddressCount()) {
            throwFatalError("No more connection addresses are left.", lastError);
        }

        int retriesLimit = getRetriesLimit();
        InetSocketAddress socketAddress = getNextSocketAddress();
        IOException connectionError = null;
        for (int i = 0; i < retriesLimit; i++) {
            try {
                return openChannel(socketAddress);
            } catch (IOException e) {
                connectionError = e;
            }
        }
        throw connectionError;
    }

    /**
     * Sets a retries count per instance.
     * 0 (infinite) count is not supported by this provider.
     *
     * @param retriesLimit limit of retries to use.
     */
    @Override
    public void setRetriesLimit(int retriesLimit) {
        if (retriesLimit == 0) {
            throwFatalError("Retries count should be at least 1 or more", null);
        }
        super.setRetriesLimit(retriesLimit);
    }

    /**
     * Gets size of addresses pool.
     *
     * @return Number of configured addresses.
     */
    protected int getAddressCount() {
        return readGuard(socketAddresses::size);
    }

    /**
     * Gets next address from the pool to be used to connect.
     *
     * @return Socket address to use for the next reconnection attempt
     */
    protected InetSocketAddress getNextSocketAddress() {
        return readGuard(() -> {
            int position = currentPosition.updateAndGet(i -> (i + 1) % socketAddresses.size());
            return socketAddresses.get(position);
        });
    }

    @Override
    public SocketAddress getAddress() {
        return readGuard(() -> {
            int position = (currentPosition.get() + 1) % socketAddresses.size();
            return socketAddresses.get(position);
        });
    }

    private <R> R readGuard(Supplier<R> supplier) {
        Lock readLock = addressListLock.readLock();
        readLock.lock();
        try {
            return supplier.get();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Update addresses pool by new list.
     *
     * @param addresses list of addresses to be applied
     *
     * @throws IllegalArgumentException if addresses list is empty
     */
    public void refreshAddresses(Collection<String> addresses) {
        updateAddressList(addresses);
    }

    private void throwFatalError(String message, Throwable lastError) {
        throw new CommunicationException(message, lastError);
    }

}