package br.com.gabriel.consumer;

public interface ServiceFactory<T> {

    ConsumerService<T> create();
}
