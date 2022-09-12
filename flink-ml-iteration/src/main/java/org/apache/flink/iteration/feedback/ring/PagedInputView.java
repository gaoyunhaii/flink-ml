/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.iteration.feedback.ring;

import org.apache.flink.core.memory.DataInputView;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base class for all input views that are backed by multiple memory pages. This base class
 * contains all decoding methods to read data from a page and detect page boundary crossing. The
 * concrete sub classes must implement the methods to provide the next memory page once the boundary
 * is crossed.
 */
public class PagedInputView implements DataInputView {

    private final ByteBuffer[] buffers;

    private int currentBuffer;

    private byte[] utfByteBuffer; // reusable byte buffer for utf-8 decoding
    private char[] utfCharBuffer; // reusable char buffer for utf-8 decoding

    // --------------------------------------------------------------------------------------------
    //                                    Constructors
    // --------------------------------------------------------------------------------------------

    public PagedInputView(ByteBuffer[] buffers) {
        this.buffers = checkNotNull(buffers);
        checkArgument(buffers.length > 1, "It should contains at least one buffer");
        this.currentBuffer = 0;
    }

    // --------------------------------------------------------------------------------------------
    //                                  Page Management
    // --------------------------------------------------------------------------------------------

    public void advance() throws IOException {
        if (currentBuffer < buffers.length - 1) {
            this.currentBuffer++;
        }

        throw new EOFException();
    }

    // --------------------------------------------------------------------------------------------
    //                               Data Input Specific methods
    // --------------------------------------------------------------------------------------------

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (off < 0 || len < 0 || off + len > b.length) {
            throw new IndexOutOfBoundsException();
        }

        if (buffers[currentBuffer].remaining() >= len) {
            buffers[currentBuffer].get(b, off, len);
            return len;
        } else {
            int bytesRead = 0;
            while (true) {
                int toRead = Math.min(buffers[currentBuffer].remaining(), len - bytesRead);
                buffers[currentBuffer].get(b, off, toRead);
                off += toRead;
                bytesRead += toRead;

                if (len > bytesRead) {
                    try {
                        advance();
                    } catch (EOFException eof) {
                        return bytesRead;
                    }
                } else {
                    break;
                }
            }

            return len;
        }
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        int bytesRead = read(b, off, len);

        if (bytesRead < len) {
            throw new EOFException("There is no enough data left in the DataInputView.");
        }
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readByte() == 1;
    }

    @Override
    public byte readByte() throws IOException {
        if (buffers[currentBuffer].hasRemaining()) {
            return buffers[currentBuffer].get();
        } else {
            advance();
            return readByte();
        }
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    @Override
    public short readShort() throws IOException {
        if (buffers[currentBuffer].remaining() > 1) {
            return buffers[currentBuffer].getShort();
        } else if (buffers[currentBuffer].remaining() == 0) {
            advance();
            return readShort();
        } else {
            return (short) ((readUnsignedByte() << 8) | readUnsignedByte());
        }
    }

    @Override
    public int readUnsignedShort() throws IOException {
        if (buffers[currentBuffer].remaining() > 1) {
            return this.buffers[currentBuffer].getShort() & 0xffff;
        } else if (buffers[currentBuffer].remaining() == 0) {
            advance();
            return readUnsignedShort();
        } else {
            return (readUnsignedByte() << 8) | readUnsignedByte();
        }
    }

    @Override
    public char readChar() throws IOException {
        if (buffers[currentBuffer].remaining() > 1) {
            return buffers[currentBuffer].getChar();
        } else if (buffers[currentBuffer].remaining() == 0) {
            advance();
            return readChar();
        } else {
            return (char) ((readUnsignedByte() << 8) | readUnsignedByte());
        }
    }

    @Override
    public int readInt() throws IOException {
        if (buffers[currentBuffer].remaining() > 3) {
            return buffers[currentBuffer].getInt();
        } else if (buffers[currentBuffer].remaining() == 0) {
            advance();
            return readInt();
        } else {
            return (readUnsignedByte() << 24)
                    | (readUnsignedByte() << 16)
                    | (readUnsignedByte() << 8)
                    | readUnsignedByte();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (buffers[currentBuffer].remaining() > 7) {
            return buffers[currentBuffer].getLong();
        } else if (buffers[currentBuffer].remaining() == 0) {
            advance();
            return readLong();
        } else {
            long l = 0L;
            l |= ((long) readUnsignedByte()) << 56;
            l |= ((long) readUnsignedByte()) << 48;
            l |= ((long) readUnsignedByte()) << 40;
            l |= ((long) readUnsignedByte()) << 32;
            l |= ((long) readUnsignedByte()) << 24;
            l |= ((long) readUnsignedByte()) << 16;
            l |= ((long) readUnsignedByte()) << 8;
            l |= (long) readUnsignedByte();
            return l;
        }
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public String readLine() throws IOException {
        final StringBuilder bld = new StringBuilder(32);

        try {
            int b;
            while ((b = readUnsignedByte()) != '\n') {
                if (b != '\r') {
                    bld.append((char) b);
                }
            }
        } catch (EOFException eofex) {
        }

        if (bld.length() == 0) {
            return null;
        }

        // trim a trailing carriage return
        int len = bld.length();
        if (len > 0 && bld.charAt(len - 1) == '\r') {
            bld.setLength(len - 1);
        }
        return bld.toString();
    }

    @Override
    public String readUTF() throws IOException {
        final int utflen = readUnsignedShort();

        final byte[] bytearr;
        final char[] chararr;

        if (this.utfByteBuffer == null || this.utfByteBuffer.length < utflen) {
            bytearr = new byte[utflen];
            this.utfByteBuffer = bytearr;
        } else {
            bytearr = this.utfByteBuffer;
        }
        if (this.utfCharBuffer == null || this.utfCharBuffer.length < utflen) {
            chararr = new char[utflen];
            this.utfCharBuffer = chararr;
        } else {
            chararr = this.utfCharBuffer;
        }

        int c, char2, char3;
        int count = 0;
        int chararrCount = 0;

        readFully(bytearr, 0, utflen);

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            if (c > 127) {
                break;
            }
            count++;
            chararr[chararrCount++] = (char) c;
        }

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx */
                    count++;
                    chararr[chararrCount++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx 10xx xxxx */
                    count += 2;
                    if (count > utflen) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    char2 = (int) bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80) {
                        throw new UTFDataFormatException("malformed input around byte " + count);
                    }
                    chararr[chararrCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx 10xx xxxx 10xx xxxx */
                    count += 3;
                    if (count > utflen) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    char2 = (int) bytearr[count - 2];
                    char3 = (int) bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                        throw new UTFDataFormatException(
                                "malformed input around byte " + (count - 1));
                    }
                    chararr[chararrCount++] =
                            (char)
                                    (((c & 0x0F) << 12)
                                            | ((char2 & 0x3F) << 6)
                                            | ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx, 1111 xxxx */
                    throw new UTFDataFormatException("malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararrCount);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        if (n < 0) {
            throw new IllegalArgumentException();
        }

        if (buffers[currentBuffer].remaining() >= n) {
            int position = buffers[currentBuffer].position();
            buffers[currentBuffer].position(position + n);
            return n;
        } else {
            int skipped = 0;
            while (true) {
                int toSKip = Math.min(buffers[currentBuffer].remaining(), n);
                n -= toSKip;
                skipped += toSKip;

                if (n > 0) {
                    try {
                        advance();
                    } catch (EOFException eofex) {
                        return skipped;
                    }
                } else {
                    break;
                }
            }

            return skipped;
        }
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        if (numBytes < 0) {
            throw new IllegalArgumentException();
        }

        if (buffers[currentBuffer].remaining() >= numBytes) {
            int position = buffers[currentBuffer].position();
            buffers[currentBuffer].position(position + numBytes);
        } else {
            while (true) {
                if (numBytes > buffers[currentBuffer].remaining()) {
                    numBytes -= buffers[currentBuffer].remaining();
                    advance();
                } else {
                    break;
                }
            }
        }
    }
}
