package br.com.gabriel;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class MessageAdapter implements JsonSerializer<Message<?>>, JsonDeserializer<Message<?>> {

    @Override
    public JsonElement serialize(Message message, Type type, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("payload", context.serialize(message.getPayload()));
        jsonObject.add("correlationId", context.serialize(message.getId()));
        jsonObject.addProperty("type", message.getPayload().getClass().getName());
        return jsonObject;
    }

    @Override
    public Message<?> deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
        var obj = jsonElement.getAsJsonObject();
        var payloadType = obj.get("type").getAsString();
        var correlationId = (CorrelationId) context.deserialize(obj.get("correlationId"), CorrelationId.class);

        try {
            var payload = context.deserialize(obj.get("payload"), Class.forName(payloadType));
            return new Message<>(correlationId, payload);
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }
    }
}
