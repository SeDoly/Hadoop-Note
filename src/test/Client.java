package test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

/**
 * Created by xiaoyunxiao on 15-4-1.
 */
public class Client {
    public static void main(String[] args) {
        Socket client1 = new Socket();
        Socket client2 = new Socket();
        InputStream input1=null;
        InputStream input2=null;
        SocketAddress serverAddress = new InetSocketAddress("localhost", 1234);
        try {
            client1.connect(serverAddress);
            input1=client1.getInputStream();
            client2.connect(serverAddress);
            input2=client2.getInputStream();
            System.out.println(input1.equals(input2));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
