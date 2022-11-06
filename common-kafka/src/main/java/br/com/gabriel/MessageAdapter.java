package br.com.gabriel;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class MessageAdapter implements JsonSerializer<Message> {

    @Override
    public JsonElement serialize(Message message, Type type, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("payload", context.serialize(message.getPayload()));
        jsonObject.add("correlationId", context.serialize(message.getId()));
        jsonObject.addProperty("type", message.getPayload().getClass().getName());
        return jsonObject;
    }
}
