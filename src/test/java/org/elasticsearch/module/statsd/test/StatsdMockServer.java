package org.elasticsearch.module.statsd.test;

import org.elasticsearch.common.collect.Lists;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Collection;

public class StatsdMockServer extends Thread {

    private int port;
    public Collection<String> content = Lists.newArrayList();
    private DatagramSocket socket;
    private boolean isClosed = false;

    public StatsdMockServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {
        try {
            socket = new DatagramSocket(port);

            while (!isClosed) {
                if (socket.isClosed()) return;

                byte[] buf = new byte[256];
                
                // receive request
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                socket.receive(packet);
                
                ByteArrayInputStream bis = new ByteArrayInputStream(buf, 0, packet.getLength());
                BufferedReader in = new BufferedReader(new InputStreamReader(bis));

                String msg;
                while ((msg = in.readLine()) != null) {
                	System.out.println("Read from socket: " + msg);
                    content.add(msg.trim());
                }
                in.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws Exception {
        isClosed = true;
        socket.close();
    }
}
