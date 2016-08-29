package yelp.search;

import java.net.*;
import java.io.*;


public class Messager extends Thread {
	private ServerSocket serverSocket1;
	private String serverName1;
	private int port1;
	
	private ServerSocket serverSocket2;
	private String serverName2;
	private int port2;

	public Messager(int port1, int port2) throws IOException {
		serverSocket1 = new ServerSocket(port1);
		serverSocket1.setSoTimeout(1000000);
		serverName1 = "localhost";
		
		serverSocket2 = new ServerSocket(port2);
		serverSocket2.setSoTimeout(1000000);
		serverName2 = "localhost";
	}
	
	public static String inputStreamAsString(InputStream stream) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
        StringBuilder sb = new StringBuilder();
        String line = null;

        while ((line = br.readLine()) != null) {
            sb.append(line + "\n");
        }

        br.close();
        return sb.toString();
    }

	public void send() {
		try {
			System.out.println("Connecting to " + serverName1 + " on port "
					+ port1);
			Socket client = new Socket(serverName1, port1);
			System.out.println("Just connected to "
					+ client.getRemoteSocketAddress());
			OutputStream outToServer = client.getOutputStream();
			DataOutputStream out = new DataOutputStream(outToServer);
			out.writeUTF("Hello from " + client.getLocalSocketAddress());
			InputStream inFromServer = client.getInputStream();
			DataInputStream in = new DataInputStream(inFromServer);
			System.out.println("Server says " + in.readUTF());
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		while (true) {
			try {
				System.out.println("Waiting for client on port "
						+ serverSocket1.getLocalPort() + "...");
				Socket server = serverSocket1.accept();
				System.out.println("Just connected to "
						+ server.getRemoteSocketAddress());
				System.out.println("Does this print anything more?");
				
				InputStream input = server.getInputStream();
				String inputString = Messager.inputStreamAsString(input);

	            System.out.println(inputString);

				//DataInputStream in = new DataInputStream(server.getInputStream());
				//System.out.println(in.readUTF());
				//DataOutputStream out = new DataOutputStream(server.getOutputStream());
				//out.writeUTF("Thank you for connecting to " + server.getLocalSocketAddress() + "\nGoodbye!");
				server.close();
				System.out.println("closing server");
			} catch (SocketTimeoutException s) {
				System.out.println("Socket timed out!");
				break;
			} catch (IOException e) {
				e.printStackTrace();
				break;
			}
		}
	}

	public static void main(String[] args) {
		try {
			Thread t = new Messager(9000, 8000);
			t.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}