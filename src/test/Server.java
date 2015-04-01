package test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * Created by xiaoyunxiao on 15-4-1.
 */
public class Server {
    public static void main(String[] args) {
        try {
            ServerSocket server = new ServerSocket();
            SocketAddress serverAddress = new InetSocketAddress("localhost", 1234);
            server.bind(serverAddress);
            Socket before = null;
            Socket now = null;
            while (true) {
                now = server.accept();
                if (before != null) {
                    System.out.println(now.equals(before));
                }
                before = now;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
