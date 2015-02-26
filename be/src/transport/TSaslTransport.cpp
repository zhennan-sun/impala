// This file will be removed when the code is accepted into the Thrift library.
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "config.h"
#ifdef HAVE_SASL_SASL_H
#include <stdint.h>
#include <sstream>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>

<<<<<<< HEAD
#include <transport/TBufferTransports.h>
#include <transport/TSaslTransport.h>
=======
#include <thrift/transport/TBufferTransports.h>
#include "transport/TSaslTransport.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

using namespace std;

namespace apache { namespace thrift { namespace transport {

<<<<<<< HEAD
  TSaslTransport::TSaslTransport(boost::shared_ptr<TTransport> transport) 
=======
  TSaslTransport::TSaslTransport(boost::shared_ptr<TTransport> transport)
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      : transport_(transport),
        memBuf_(new TMemoryBuffer()),
        shouldWrap_(false),
        isClient_(false) {
  }

  TSaslTransport::TSaslTransport(boost::shared_ptr<sasl::TSasl> saslClient,
                                 boost::shared_ptr<TTransport> transport)
      : transport_(transport),
        memBuf_(new TMemoryBuffer()),
        sasl_(saslClient),
        shouldWrap_(false),
        isClient_(true) {
  }

  TSaslTransport::~TSaslTransport() {
    delete memBuf_;
  }

  bool TSaslTransport::isOpen() {
    return transport_->isOpen();
  }

  bool TSaslTransport::peek(){
    return (transport_->peek());
  }

<<<<<<< HEAD
=======
  string TSaslTransport::getUsername() {
    return sasl_->getUsername();
  }

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  void TSaslTransport::sendSaslMessage(const NegotiationStatus status,
      const uint8_t* payload, const uint32_t length, bool flush) {
    uint8_t messageHeader[STATUS_BYTES + PAYLOAD_LENGTH_BYTES];
    uint8_t dummy = 0;
    if (payload == NULL) {
      payload = &dummy;
    }
    messageHeader[0] = (uint8_t)status;
    encodeInt(length, messageHeader, STATUS_BYTES);
    transport_->write(messageHeader, HEADER_LENGTH);
    transport_->write(payload, length);
    if (flush) transport_->flush();
  }

  void TSaslTransport::open() {
    NegotiationStatus status = TSASL_INVALID;
    uint32_t resLength;

<<<<<<< HEAD
    if (!transport_->isOpen()) {
=======
    // Only client should open the underlying transport.
    if (isClient_ && !transport_->isOpen()) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      transport_->open();
    }

    // initiate  SASL message
    handleSaslStartMessage();

    // SASL connection handshake
    while (!sasl_->isComplete()) {
      uint8_t* message = receiveSaslMessage(&status, &resLength);
      if (status == TSASL_COMPLETE) {
        if (isClient_) break; // handshake complete
      } else if (status != TSASL_OK) {
        stringstream ss;
        ss << "Expected COMPLETE or OK, got " << status;
        throw TTransportException(ss.str());
      }
      uint32_t challengeLength;
      uint8_t* challenge = sasl_->evaluateChallengeOrResponse(
          message, resLength, &challengeLength);
      sendSaslMessage(sasl_->isComplete() ? TSASL_COMPLETE : TSASL_OK,
                      challenge, challengeLength);
    }

    // If the server isn't complete yet, we need to wait for its response.
    // This will occur with ANONYMOUS auth, for example, where we send an
    // initial response and are immediately complete.
    if (isClient_ && (status == TSASL_INVALID || status == TSASL_OK)) {
      receiveSaslMessage(&status, &resLength);
      if (status != TSASL_COMPLETE) {
        stringstream ss;
        ss << "Expected COMPLETE or OK, got " << status;
        throw TTransportException(ss.str());
      }
    }

    // TODO : need to set the shouldWrap_ based on QOP
    /*
    String qop = (String) sasl.getNegotiatedProperty(Sasl.QOP);
    if (qop != null && !qop.equalsIgnoreCase("auth"))
      shouldWrap_ = true;
    */
  }

  void TSaslTransport::close() {
    transport_->close();
  }

  uint32_t TSaslTransport::readLength() {
    uint8_t lenBuf[PAYLOAD_LENGTH_BYTES];

    transport_->readAll(lenBuf, PAYLOAD_LENGTH_BYTES);
    int32_t len = decodeInt(lenBuf, 0);
    if (len < 0) {
      throw TTransportException("Frame size has negative value");
    }
    return static_cast<uint32_t>(len);
  }

  uint32_t TSaslTransport::read(uint8_t* buf, uint32_t len) {
<<<<<<< HEAD

    // if there's not enough data in cache, read from underlying transport
    if (memBuf_->available_read() < len) {
      uint32_t dataLength = readLength();

      // Fast path
      if (len == dataLength && !shouldWrap_) {
        transport_->readAll(buf, len);
        return len;
      }

      uint8_t* tmpBuf = new uint8_t[dataLength];
      transport_->readAll(tmpBuf, dataLength);
      if (shouldWrap_) {
        tmpBuf = sasl_->unwrap(tmpBuf, 0, dataLength, &dataLength);
      }

      // We will consume all the data, no need to put it in the memory buffer.
      if (len == dataLength) {
        memcpy(buf, tmpBuf, len);
        delete tmpBuf;
        return len;
      }

      memBuf_->write(tmpBuf, dataLength);
      memBuf_->flush();
      delete tmpBuf;
    }
=======
    uint32_t read_bytes = memBuf_->read(buf, len);
    if (read_bytes > 0) return read_bytes;

    // if there's not enough data in cache, read from underlying transport
    uint32_t dataLength = readLength();

    // Fast path
    if (len == dataLength && !shouldWrap_) {
      transport_->readAll(buf, len);
      return len;
    }

    uint8_t* tmpBuf = new uint8_t[dataLength];
    transport_->readAll(tmpBuf, dataLength);
    if (shouldWrap_) {
      tmpBuf = sasl_->unwrap(tmpBuf, 0, dataLength, &dataLength);
    }

    // We will consume all the data, no need to put it in the memory buffer.
    if (len == dataLength) {
      memcpy(buf, tmpBuf, len);
      delete tmpBuf;
      return len;
    }

    memBuf_->write(tmpBuf, dataLength);
    memBuf_->flush();
    delete tmpBuf;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    return memBuf_->read(buf, len);
  }

  void TSaslTransport::writeLength(uint32_t length) {
    uint8_t lenBuf[PAYLOAD_LENGTH_BYTES];

    encodeInt(length, lenBuf, 0);
    transport_->write(lenBuf, PAYLOAD_LENGTH_BYTES);
  }

  void TSaslTransport::write(const uint8_t* buf, uint32_t len) {
    const uint8_t* newBuf;

    if (shouldWrap_) {
      newBuf = sasl_->wrap((uint8_t*)buf, 0, len, &len);
    } else {
      newBuf = buf;
    }
    writeLength(len);
    transport_->write(newBuf, len);
  }

  void TSaslTransport::flush() {
    transport_->flush();
  }

  uint8_t* TSaslTransport::receiveSaslMessage(NegotiationStatus* status,
                                              uint32_t* length) {
    uint8_t messageHeader[HEADER_LENGTH];

    // read header
    transport_->readAll(messageHeader, HEADER_LENGTH);

    // get payload status
    *status = (NegotiationStatus)messageHeader[0];
    if ((*status < TSASL_START) || (*status > TSASL_COMPLETE)) {
      throw TTransportException("invalid sasl status");
    } else if (*status == TSASL_BAD || *status == TSASL_ERROR) {
        throw TTransportException("sasl Peer indicated failure: ");
    }

    // get the length
    *length = decodeInt(messageHeader, STATUS_BYTES);

    // get payload
    protoBuf_.reset(new uint8_t[*length]);
    transport_->readAll(protoBuf_.get(), *length);

    return protoBuf_.get();
  }
}
}
}

#endif
