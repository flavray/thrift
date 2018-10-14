package org.apache.thrift.transport;

/**
 * TTransport backed by a growable byte array.
 *
 * This transport can be both read from and written into.
 * NOTE: it is assumed that no interleaved read/write operations are performed.
 * All read operation must happen before any write operation, and vice-versa.
 *
 * This can be useful for server implementation where a single "buffer' will be
 * used to read and write data.
 */
public class TMemoryTransport extends TTransport {

  private enum State {
    INIT,
    READING,
    WRITING
  }

  private byte[] buf_;
  private int pos_;
  private State state_ = State.INIT;

  public TMemoryTransport() { }

  public void reset(byte[] buf) {
    buf_ = buf;
    pos_ = 0;
    state_ = State.INIT;
  }

  public void clear() {
    buf_ = null;
  }

  @Override
  public void close() {}

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public void open() throws TTransportException {}

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    if (state_ != State.READING) {
      pos_ = 0;
      state_ = State.READING;
    }

    int bytesRemaining = getBytesRemainingInBuffer();
    int amtToRead = (len > bytesRemaining ? bytesRemaining : len);
    if (amtToRead > 0) {
      System.arraycopy(buf_, pos_, buf, off, amtToRead);
      consumeBuffer(amtToRead);
    }
    return amtToRead;
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    if (state_ != State.WRITING) {
      pos_ = 0;
      state_ = State.WRITING;
    }

    ensureCapacity(len);
    System.arraycopy(buf, off, buf_, pos_, len);
    consumeBuffer(len);
  }

  @Override
  public byte[] getBuffer() {
    return buf_;
  }

  @Override
  public int getBufferPosition() {
    return pos_;
  }

  @Override
  public int getBytesRemainingInBuffer() {
    return buf_.length - pos_;
  }

  @Override
  public void consumeBuffer(int len) {
    pos_ += len;
  }

  public boolean isWriting() {
    return state_ == State.WRITING;
  }

  protected void ensureCapacity(int capacity) {
    if (pos_ + capacity > buf_.length) {
      grow(capacity);
    }
  }

  protected void grow(int capacity) {
    byte[] newBuf = new byte[capacity];
    System.arraycopy(buf_, 0, newBuf, 0, pos_);
    buf_ = newBuf;
  }

}
