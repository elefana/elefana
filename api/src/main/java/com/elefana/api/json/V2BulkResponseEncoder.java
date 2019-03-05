package com.elefana.api.json;

import com.elefana.api.document.BulkItemResponse;
import com.elefana.api.document.BulkResponse;
import com.jsoniter.output.JsonStream;
import com.jsoniter.spi.Encoder;

import java.io.IOException;

public class V2BulkResponseEncoder implements Encoder {

	@Override
	public void encode(Object obj, JsonStream stream) throws IOException {
		stream.writeObjectStart();

		BulkResponse bulkResponse = (BulkResponse) obj;
		stream.writeObjectField("took");
		stream.writeVal(bulkResponse.getTook());
		stream.writeMore();

		stream.writeObjectField("errors");
		stream.writeVal(bulkResponse.isErrors());
		stream.writeMore();

		stream.writeObjectField("items");
		stream.writeArrayStart();

		for(int i = 0; i < bulkResponse.getItems().size(); i++) {
			BulkItemResponse itemResponse = bulkResponse.getItems().get(i);
			stream.writeObjectStart();

			switch(itemResponse.getOpType()) {
			case DELETE:
				stream.writeObjectField("delete");
				break;
			case INDEX:
			default:
				stream.writeObjectField("create");
				break;
			}

			stream.writeObjectStart();
			stream.writeObjectField("_index");
			stream.writeVal(itemResponse.getIndex());
			stream.writeMore();

			stream.writeObjectField("_type");
			stream.writeVal(itemResponse.getType());
			stream.writeMore();

			stream.writeObjectField("_id");
			stream.writeVal(itemResponse.getId());
			stream.writeMore();

			stream.writeObjectField("result");
			stream.writeVal(itemResponse.getResult());

			if(!itemResponse.isFailed()) {
				stream.writeMore();
				stream.writeObjectField("_version");
				stream.writeVal(itemResponse.getVersion());

				stream.writeMore();
				stream.writeObjectField("status");

				switch(itemResponse.getOpType()) {
				case DELETE:
					stream.writeVal(200);
					break;
				case INDEX:
				default:
					stream.writeVal(201);
					break;
				}
			} else {
				stream.writeMore();
				stream.writeObjectField("status");
				stream.writeVal(500);
			}

			stream.writeObjectEnd();
			stream.writeObjectEnd();

			if(i < bulkResponse.getItems().size() - 1) {
				stream.writeMore();
			}
		}

		stream.writeArrayEnd();

		stream.writeObjectEnd();
	}
}