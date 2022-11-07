package br.com.gabriel;

public interface ServiceFactory<T> {

    ConsumerService<T> create();
}
