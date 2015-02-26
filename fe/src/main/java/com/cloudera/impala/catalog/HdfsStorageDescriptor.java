// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
<<<<<<< HEAD
import org.apache.hadoop.hive.serde.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.thrift.THdfsCompression;
=======
import org.apache.hadoop.hive.serde.serdeConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Represents the file format metadata for files stored in a table or partition.
 */
public class HdfsStorageDescriptor {
  private static final char DEFAULT_LINE_DELIM = '\n';
  // hive by default uses ctrl-a as field delim
  private static final char DEFAULT_FIELD_DELIM = '\u0001';
  // hive by default has no escape char
<<<<<<< HEAD
  private static final char DEFAULT_ESCAPE_CHAR = '\u0000';

  private final HdfsFileFormat fileFormat;

  private final char lineDelim;
  private final char fieldDelim;
  private final char collectionDelim;
  private final char mapKeyDelim;
  private final char escapeChar;
  private final char quoteChar;
  private final int blockSize;
  private final THdfsCompression compression;
=======
  public static final char DEFAULT_ESCAPE_CHAR = '\u0000';
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Serde parameters that are recognized by table writers.
  private static final String BLOCK_SIZE = "blocksize";
  private static final String COMPRESSION = "compression";

  // Important: don't change the ordering of these keys - if e.g. FIELD_DELIM is not
  // found, the value of LINE_DELIM is used, so LINE_DELIM must be found first.
<<<<<<< HEAD
  final static List<String> DELIMITER_KEYS =
      ImmutableList.of(Constants.LINE_DELIM, Constants.FIELD_DELIM,
        Constants.COLLECTION_DELIM, Constants.MAPKEY_DELIM, Constants.ESCAPE_CHAR,
        Constants.QUOTE_CHAR);

  private final static Logger LOG = LoggerFactory.getLogger(HdfsStorageDescriptor.class);

=======
  // Package visible for testing.
  final static List<String> DELIMITER_KEYS = ImmutableList.of(
      serdeConstants.LINE_DELIM, serdeConstants.FIELD_DELIM,
      serdeConstants.COLLECTION_DELIM, serdeConstants.MAPKEY_DELIM,
      serdeConstants.ESCAPE_CHAR, serdeConstants.QUOTE_CHAR);

  // The Parquet serde shows up multiple times as the location of the implementation
  // has changed between Impala versions.
  final static List<String> COMPATIBLE_SERDES = ImmutableList.of(
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", // (seq / text / parquet)
      "org.apache.hadoop.hive.serde2.avro.AvroSerDe", // (avro)
      "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe", // (rc)
      "parquet.hive.serde.ParquetHiveSerDe", // (parquet - legacy)
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"); // (parquet)

  private final static Logger LOG = LoggerFactory.getLogger(HdfsStorageDescriptor.class);

  private final HdfsFileFormat fileFormat_;
  private final byte lineDelim_;
  private final byte fieldDelim_;
  private final byte collectionDelim_;
  private final byte mapKeyDelim_;
  private final byte escapeChar_;
  private final byte quoteChar_;
  private final int blockSize_;

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  /**
   * Returns a map from delimiter key to a single delimiter character,
   * filling in defaults if explicit values are not found in the supplied
   * serde descriptor.
   *
   * @throws InvalidStorageDescriptorException - if an invalid delimiter is found
   */
<<<<<<< HEAD
  private static Map<String, Character> extractDelimiters(SerDeInfo serdeInfo)
=======
  private static Map<String, Byte> extractDelimiters(SerDeInfo serdeInfo)
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      throws InvalidStorageDescriptorException {
    // The metastore may return null for delimiter parameters,
    // which means we need to use a default instead.
    // We tried long and hard to find default values for delimiters in Hive,
    // but could not find them.
<<<<<<< HEAD
    Map<String, Character> delimMap = Maps.newHashMap();
=======
    Map<String, Byte> delimMap = Maps.newHashMap();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

    for (String delimKey: DELIMITER_KEYS) {
      String delimValue = serdeInfo.getParameters().get(delimKey);
      if (delimValue == null) {
<<<<<<< HEAD
        if (delimKey.equals(Constants.FIELD_DELIM)) {
          delimMap.put(delimKey, DEFAULT_FIELD_DELIM);
        } else if (delimKey.equals(Constants.ESCAPE_CHAR)) {
          delimMap.put(delimKey, DEFAULT_ESCAPE_CHAR);
        } else if (delimKey.equals(Constants.LINE_DELIM)) {
          delimMap.put(delimKey, DEFAULT_LINE_DELIM);
        } else {
          delimMap.put(delimKey, delimMap.get(Constants.FIELD_DELIM));
        }
      } else {
        if (delimValue.length() != 1) {
          throw new InvalidStorageDescriptorException("Invalid delimiter: " + delimKey +
              " has invalid length (" + delimValue + ")"
              + ". Already found: " + delimMap);
        }

        delimMap.put(delimKey, delimValue.charAt(0));
      }
    }

    return delimMap;
  }

  public char getLineDelim() {
    return lineDelim;
  }

  public char getFieldDelim() {
    return fieldDelim;
  }

  public char getCollectionDelim() {
    return collectionDelim;
  }

  public char getMapKeyDelim() {
    return mapKeyDelim;
  }

  public char getEscapeChar() {
    return escapeChar;
  }

  public char getQuoteChar() {
    return quoteChar;
  }

  public HdfsFileFormat getFileFormat() {
    return fileFormat;
  }

  public int getBlockSize(){
    return blockSize;
  }

  public THdfsCompression getCompression() {
    return compression;
  }

  public HdfsStorageDescriptor(HdfsFileFormat fileFormat, char lineDelim,
      char fieldDelim, char collectionDelim, char mapKeyDelim, char escapeChar,
      char quoteChar, int blockSize, THdfsCompression compression) {
    this.fileFormat = fileFormat;
    this.lineDelim = lineDelim;
    this.fieldDelim = fieldDelim;
    this.collectionDelim = collectionDelim;
    this.mapKeyDelim = mapKeyDelim;
    this.escapeChar = escapeChar;
    this.quoteChar = quoteChar;
    this.blockSize = blockSize;
    this.compression = compression;
  }

  /**
   * Thrown when constructing an HdfsStorageDescriptor from an invalid
   * metatore storage descriptor.
   */
  public static class InvalidStorageDescriptorException extends Exception {
    // Mandatory since Exception implements Serialisable
    private static final long serialVersionUID = -555234913768134760L;
    public InvalidStorageDescriptorException(String s) { super(s); }
    public InvalidStorageDescriptorException(Exception ex) { super(ex); }
=======
        if (delimKey.equals(serdeConstants.FIELD_DELIM)) {
          delimMap.put(delimKey, (byte) DEFAULT_FIELD_DELIM);
        } else if (delimKey.equals(serdeConstants.ESCAPE_CHAR)) {
          delimMap.put(delimKey, (byte) DEFAULT_ESCAPE_CHAR);
        } else if (delimKey.equals(serdeConstants.LINE_DELIM)) {
          delimMap.put(delimKey, (byte) DEFAULT_LINE_DELIM);
        } else {
          delimMap.put(delimKey, delimMap.get(serdeConstants.FIELD_DELIM));
        }
      } else {
        Byte delimByteValue = parseDelim(delimValue);
        if (delimByteValue == null) {
          throw new InvalidStorageDescriptorException("Invalid delimiter: '" +
              delimValue + "'. Delimiter must be specified as a single character or " +
              "as a decimal value in the range [-128:127]");
        }
        delimMap.put(delimKey, parseDelim(delimValue));
      }
    }
    return delimMap;
  }

  /**
   * Parses a delimiter in a similar way as Hive, with some additional error checking.
   * A delimiter must fit in a single byte and can be specified in the following
   * formats, as far as I can tell (there isn't documentation):
   * - A single ASCII or unicode character (ex. '|')
   * - An escape character in octal format (ex. \001. Stored in the metastore as a
   *   unicode character: \u0001).
   * - A signed decimal integer in the range [-128:127]. Used to support delimiters
   *   for ASCII character values between 128-255 (-2 maps to ASCII 254).
   *
   * The delimiter is first parsed as a decimal number. If the parsing succeeds AND
   * the resulting value fits in a signed byte, the byte value of the parsed int is
   * returned. Otherwise, if the string has a single char, the byte value of this
   * char is returned.
   * If the delimiter is invalid, null will be returned.
   */
  public static Byte parseDelim(String delimVal) {
    Preconditions.checkNotNull(delimVal);
    try {
      // In the future we could support delimiters specified in hex format, but we would
      // need support from the Hive side.
      return Byte.parseByte(delimVal);
    } catch (NumberFormatException e) {
      if (delimVal.length() == 1) return (byte) delimVal.charAt(0);
    }
    return null;
  }

  public HdfsStorageDescriptor(String tblName, HdfsFileFormat fileFormat, byte lineDelim,
      byte fieldDelim, byte collectionDelim, byte mapKeyDelim, byte escapeChar,
      byte quoteChar, int blockSize) {
    this.fileFormat_ = fileFormat;
    this.lineDelim_ = lineDelim;
    this.fieldDelim_ = fieldDelim;
    this.collectionDelim_ = collectionDelim;
    this.mapKeyDelim_ = mapKeyDelim;
    this.quoteChar_ = quoteChar;
    this.blockSize_ = blockSize;

    // You can set the escape character as a tuple or row delim.  Empirically,
    // this is ignored by hive.
    if (escapeChar == fieldDelim ||
        escapeChar == lineDelim ||
        escapeChar == collectionDelim) {
      // TODO: we should output the table name here but it's hard to get to now.
      this.escapeChar_ = DEFAULT_ESCAPE_CHAR;
      LOG.warn("Escape character for table, " + tblName + " is set to "
          + "the same character as one of the delimiters.  Ignoring escape character.");
    } else {
      this.escapeChar_ = escapeChar;
    }
  }

  /**
   * Thrown when constructing an HdfsStorageDescriptor from an invalid/unsupported
   * metastore storage descriptor.
   * TODO: Get rid of this class.
   */
  public static class InvalidStorageDescriptorException extends CatalogException {
    // Mandatory since Exception implements Serialisable
    private static final long serialVersionUID = -555234913768134760L;
    public InvalidStorageDescriptorException(String s) { super(s); }
    public InvalidStorageDescriptorException(Exception ex) {
      super(ex.getMessage(), ex);
    }
    public InvalidStorageDescriptorException(String msg, Throwable cause) {
      super(msg, cause);
    }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  /**
   * Constructs a new HdfsStorageDescriptor from a StorageDescriptor retrieved from the
   * metastore.
   *
   * @throws InvalidStorageDescriptorException - if the storage descriptor has invalid
<<<<<<< HEAD
   * delimiters, or an unknown file format.
   */
  public static HdfsStorageDescriptor fromStorageDescriptor(StorageDescriptor sd)
      throws InvalidStorageDescriptorException {
    Map<String, Character> delimMap = extractDelimiters(sd.getSerdeInfo());

=======
   * delimiters, an unsupported SerDe, or an unknown file format.
   */
  public static HdfsStorageDescriptor fromStorageDescriptor(String tblName,
      StorageDescriptor sd)
      throws InvalidStorageDescriptorException {
    Map<String, Byte> delimMap = extractDelimiters(sd.getSerdeInfo());
    if (!COMPATIBLE_SERDES.contains(sd.getSerdeInfo().getSerializationLib())) {
      throw new InvalidStorageDescriptorException(String.format("Impala does not " +
          "support tables of this type. REASON: SerDe library '%s' is not " +
          "supported.", sd.getSerdeInfo().getSerializationLib()));
    }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    // Extract the blocksize and compression specification from the SerDe parameters,
    // if present.
    Map<String, String> parameters = sd.getSerdeInfo().getParameters();
    int blockSize = 0;
    String blockValue = parameters.get(BLOCK_SIZE);
    if (blockValue != null) {
      blockSize = Integer.parseInt(blockValue);
    }
<<<<<<< HEAD
    THdfsCompression compression = THdfsCompression.NONE;
    String compressionValue = parameters.get(COMPRESSION);
    if (compressionValue != null) {
      if (com.cloudera.impala.thrift.Constants.COMPRESSION_MAP.containsKey(
            compressionValue)) {
        compression =
          com.cloudera.impala.thrift.Constants.COMPRESSION_MAP.get(compressionValue);
      } else {
        LOG.warn("Unknown compression type: " + compressionValue);
      }
    }

    try {
      return new HdfsStorageDescriptor(
          HdfsFileFormat.fromJavaClassName(sd.getInputFormat()),
          delimMap.get(Constants.LINE_DELIM), delimMap.get(Constants.FIELD_DELIM),
          delimMap.get(Constants.COLLECTION_DELIM), delimMap.get(Constants.MAPKEY_DELIM),
          delimMap.get(Constants.ESCAPE_CHAR), delimMap.get(Constants.QUOTE_CHAR),
          blockSize, compression);
=======

    try {
      return new HdfsStorageDescriptor(tblName,
          HdfsFileFormat.fromJavaClassName(sd.getInputFormat()),
          delimMap.get(serdeConstants.LINE_DELIM),
          delimMap.get(serdeConstants.FIELD_DELIM),
          delimMap.get(serdeConstants.COLLECTION_DELIM),
          delimMap.get(serdeConstants.MAPKEY_DELIM),
          delimMap.get(serdeConstants.ESCAPE_CHAR),
          delimMap.get(serdeConstants.QUOTE_CHAR),
          blockSize);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    } catch (IllegalArgumentException ex) {
      // Thrown by fromJavaClassName
      throw new InvalidStorageDescriptorException(ex);
    }
  }
<<<<<<< HEAD
=======

  public byte getLineDelim() { return lineDelim_; }
  public byte getFieldDelim() { return fieldDelim_; }
  public byte getCollectionDelim() { return collectionDelim_; }
  public byte getMapKeyDelim() { return mapKeyDelim_; }
  public byte getEscapeChar() { return escapeChar_; }
  public byte getQuoteChar() { return quoteChar_; }
  public HdfsFileFormat getFileFormat() { return fileFormat_; }
  public int getBlockSize() { return blockSize_; }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
