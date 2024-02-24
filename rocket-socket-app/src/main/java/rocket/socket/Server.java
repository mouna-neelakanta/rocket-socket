package rocket.socket;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import io.rsocket.core.RSocketServer;

import java.math.BigInteger;

import io.rsocket.Payload;
import io.rsocket.RSocket;

public class Server 
{
    private final Disposable server;

    public static void main(String[] args) {
        new Server();
    }

    public Server()
    {
        server = RSocketServer.create((setup, sendingSocket) -> Mono.just(new MyRSocket()))
        .bind(WebsocketServerTransport.create("localhost", 7000))
        .doOnNext(conn -> System.out.println("Connected to " + conn.address()))
        .subscribe();

        //keep the main thread alive to keep the server running 
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } 
    }

    public void dispose(){
        this.server.dispose();
    }

    private class MyRSocket implements RSocket{
        @Override
        public Mono<Void> fireAndForget(io.rsocket.Payload payload) {
            byte[] bytes = new byte[payload.data().readableBytes()];
            payload.data().readBytes(bytes);
            System.out.println("Received: {}" + new BigInteger(bytes).intValue());
            return Mono.empty();
        }

        @Override
        public Mono<Payload> requestResponse(Payload payload)
        {
            byte[] bytes = new byte[payload.data().readableBytes()];
            payload.data().readBytes(bytes);
            System.out.println("Received: {}" + new BigInteger(bytes).intValue());
            String response = "I have recieved the request";
            return Mono.just(DefaultPayload.create(response));

        }

    }
}
