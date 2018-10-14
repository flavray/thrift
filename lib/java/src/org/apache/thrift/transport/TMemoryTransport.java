package org.apache.thrift.transport;

import java.util.Arrays;

public class TMemoryTransport extends TTransport {

  private enum State {
    INIT,
    READING,
    WRITING
  }

  private byte[] buf_;
  private int pos_;
  private int endPos_;
  private int resetPos_;
  private State state_ = State.INIT;

  public TMemoryTransport() {
  }

  public TMemoryTransport(byte[] buf) {
    reset(buf);
  }

  public TMemoryTransport(byte[] buf, int offset, int length) {
    reset(buf, offset, length);
  }

  public void reset(byte[] buf) {
    reset(buf, 0, buf.length);
  }

  public void reset(byte[] buf, int offset, int length) {
    buf_ = buf;
    pos_ = offset;
    endPos_ = offset + length;
    resetPos_ = pos_;
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
      pos_ = resetPos_;
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
      pos_ = resetPos_;
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

  public int getBufferPosition() {
    return pos_;
  }

  public int getBytesRemainingInBuffer() {
    return endPos_ - pos_;
  }

  public void consumeBuffer(int len) {
    pos_ += len;
  }

  public boolean isWriting() {
    return state_ == State.WRITING;
  }

  protected void ensureCapacity(int capacity) {
    if (endPos_ - pos_ < capacity) {
      grow(capacity);
    }
  }

  protected void grow(int capacity) {
    byte[] newBuf = new byte[capacity];
    System.arraycopy(buf_, resetPos_, newBuf, 0, pos_- resetPos_);
    reset(newBuf);
  }

}
