// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.firebase.firestore.bundle;

import androidx.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.CharBuffer;
import org.json.JSONException;

public class BundleReader {
  private static final int INITIAL_BUFFER_CAPACITY = 1024;

  @Nullable BundleMetadata metadata;

  private final BundleSerializer serializer;
  private final BufferedReader dataReader;
  private CharBuffer buffer;
  long bytesRead;

  public BundleReader(BundleSerializer serializer, InputStream data) {
    this.serializer = serializer;
    dataReader = new BufferedReader(new InputStreamReader(data));
    buffer = CharBuffer.allocate(INITIAL_BUFFER_CAPACITY);
  }

  public void close() throws IOException {
    dataReader.close();
  }

  public BundleMetadata getBundleMetadata() throws IOException, JSONException {
    if (metadata != null) {
      return metadata;
    }
    BundleElement element = readNextElement();
    if (!(element instanceof BundleMetadata)) {
      throw new IllegalArgumentException(
          "Expected first element in bundle to be a metadata object");
    }
    return metadata;
  }

  public BundleElement getNextElement() throws IOException, JSONException {
    // Makes sure metadata is read before proceeding.
    getBundleMetadata();
    return readNextElement();
  }

  public long getBytesRead() {
    return bytesRead;
  }

  /**
   * Reads from the head of internal buffer, and pulling more data from underlying stream if a
   * complete element cannot be found, until an element(including the prefixed length and the JSON
   * string) is found.
   *
   * <p>Once a complete element is read, it is dropped from internal buffer.
   *
   * <p>Returns either the bundled element, or null if we have reached the end of the stream.
   */
  private BundleElement readNextElement() throws IOException, JSONException {
    int length = readLength();
    if (length == -1) {
      return null;
    }

    String json = readJsonString(length);
    return BundleElement.fromJson(serializer, json);
  }

  /**
   * Reads from the beginning of the internal buffer, until the first '{', and return the content.
   *
   * <p>If reached end of the stream, returns -1.
   */
  private int readLength() throws IOException {
    int positionOfOpenBracket = -1;

    while ((positionOfOpenBracket = indexOfOpenBracket()) == -1) {
      if (!pullMoreDataToBuffer(1)) {
        // Broke out of the loop because underlying stream is closed, and there
        // happens to be no more data to process.
        if (buffer.length() == 0) {
          return -1;
        }

        // Broke out of the loop because underlying stream is closed, but still
        // cannot find an open bracket.
        if (buffer.charAt(buffer.length()) != '{') {
          raiseError("Reached the end of bundle when a length string is expected.");
        }
      }
    }

    String result = buffer.subSequence(0, positionOfOpenBracket).toString();
    buffer.position(buffer.position() + positionOfOpenBracket);
    return Integer.parseInt(result.toString());
  }

  private int indexOfOpenBracket() {
    int lastPosition = buffer.position();
    for (int i = lastPosition; i < buffer.length(); ++i) {
      if (buffer.charAt(i) == '{') {
        buffer.position(lastPosition);
        return i - lastPosition;
      }
    }
    return -1;
  }

  /**
   * Reads from a specified position from the internal buffer, for a specified number of bytes,
   * pulling more data from the underlying stream if needed.
   *
   * <p>Returns a string decoded from the read bytes.
   */
  private String readJsonString(int length) throws IOException {
    if (buffer.length() < length) {
      pullMoreDataToBuffer(length - buffer.length());
    }

    if (buffer.length() < length) {
      raiseError("Reached the end of bundle when more is expected.");
    }

    String result = buffer.subSequence(0, length).toString();
    buffer.position(buffer.position() + length);
    return result;
  }

  private void raiseError(String message) throws IOException {
    close();
    throw new IllegalArgumentException("Invalid bundle format: " + message);
  }

  /** Pulls more data from underlying stream to internal buffer. Returns whether data was read */
  private boolean pullMoreDataToBuffer(int length) throws IOException {
    if (buffer.remaining() < length) {
      CharBuffer existingData = buffer;
      buffer = CharBuffer.allocate(length);
      buffer.append(existingData);
    }

    int charactersRead = 0;
    int read = -1;

    while (charactersRead < length && (read = dataReader.read(buffer)) != -1) {
      charactersRead += read;
    }

    return charactersRead != 0;
  }
}
