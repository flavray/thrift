package org.apache.thrift.transport;

import java.nio.channels.spi.SelectorProvider;
import jnr.enxio.channels.NativeSelectorProvider;
import jnr.unixsocket.UnixServerSocket;
import jnr.unixsocket.UnixServerSocketChannel;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TNonblockingUnixServerSocket extends TNonblockingServerTransport {
    private static final Logger LOGGER = LoggerFactory.getLogger(TNonblockingUnixServerSocket.class.getName());

    /**
     * This channel is where all the nonblocking magic happens.
     */
    private UnixServerSocketChannel serverSocketChannel = null;

    /**
     * Underlying ServerSocket object
     */
    private UnixServerSocket serverSocket_ = null;

    /**
     * Timeout for client sockets from accept
     */
    private int clientTimeout_ = 0;

    public static class NonblockingAbstractUnixServerSocketArgs extends
            AbstractServerTransportArgs<NonblockingAbstractUnixServerSocketArgs> {
        UnixSocketAddress socketAddress;

        public NonblockingAbstractUnixServerSocketArgs socketAddress(UnixSocketAddress socketAddress) {
            this.socketAddress = socketAddress;
            return this;
        }
    }

    public TNonblockingUnixServerSocket(UnixSocketAddress socketAddress) throws TTransportException {
        this(socketAddress, 0);
    }

    public TNonblockingUnixServerSocket(UnixSocketAddress socketAddress, int clientTimeout) throws TTransportException {
        this(new NonblockingAbstractUnixServerSocketArgs().socketAddress(socketAddress).clientTimeout(clientTimeout));
    }

    public TNonblockingUnixServerSocket(NonblockingAbstractUnixServerSocketArgs args) throws TTransportException {
        clientTimeout_ = args.clientTimeout;
        try {
            serverSocketChannel = UnixServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);

            // Make server socket
            serverSocket_ = serverSocketChannel.socket();
            serverSocket_.bind(args.socketAddress, args.backlog);
        } catch (IOException ioe) {
            serverSocket_ = null;
            throw new TTransportException("Could not create ServerSocket on unix socket " + args.socketAddress.toString() + ".", ioe);
        }
    }

    public void listen() throws TTransportException {
        // Nothing to do here
    }

    protected TNonblockingSocket acceptImpl() throws TTransportException {
        if (serverSocket_ == null) {
            throw new TTransportException(TTransportException.NOT_OPEN, "No underlying server socket.");
        }
        try {
            UnixSocketChannel socketChannel = serverSocketChannel.accept();
            if (socketChannel == null) {
                return null;
            }

            TNonblockingSocket tsocket = new TNonblockingSocket(socketChannel);
            tsocket.setTimeout(clientTimeout_);
            return tsocket;
        } catch (IOException iox) {
            throw new TTransportException(iox);
        }
    }

    @Override
    public SelectorProvider selectorProvider() {
        return NativeSelectorProvider.getInstance();
    }

    @Override
    public void registerSelector(Selector selector) {
        try {
            // Register the server socket channel, indicating an interest in
            // accepting new connections
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (ClosedChannelException e) {
            // this shouldn't happen, ideally...
            // TODO: decide what to do with this.
        }
    }

    public void close() {
        serverSocket_ = null;
    }

    public void interrupt() {
        // The thread-safeness of this is dubious, but Java documentation suggests
        // that it is safe to do this from a different thread context
        close();
    }
}
