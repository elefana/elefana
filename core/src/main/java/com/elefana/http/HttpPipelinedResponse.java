/**
 * This software is licensed under the Apache 2 license, quoted below.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with
 * the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package com.elefana.http;

import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponse;

/**
 * Based on https://github.com/typesafehub/netty-http-pipelining and
 * https://github.com/spinscale/netty4-http-pipelining
 */
public class HttpPipelinedResponse implements Comparable<HttpPipelinedResponse> {

    private final HttpResponse response;
    private final ChannelPromise promise;
    private final int sequenceId;

    public HttpPipelinedResponse(HttpResponse response, ChannelPromise promise, int sequenceId) {
        this.response = response;
        this.promise = promise;
        this.sequenceId = sequenceId;
    }

    public int getSequenceId() {
        return sequenceId;
    }

    public HttpResponse getResponse() {
        return response;
    }

    public ChannelPromise getPromise() {
        return promise;
    }

    @Override
    public int compareTo(HttpPipelinedResponse other) {
        return this.sequenceId - other.getSequenceId();
    }
}
