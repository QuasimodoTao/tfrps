package tfrps;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

class Pack {
	public static final int TYPE_HANDSHAKE = 1;
	public static final int TYPE_KEEPALIVE = 2;
	public static final int TYPE_START_CONNECT = 3;
	public static final int TYPE_STOP_CONNECT = 4;
	public static final int TYPE_DATA = 5;
	public static final int TYPE_DISCONNECT = 6;
	public int type = 0;
	public int readPos = 0;
	public int dataSize = 0;
	public byte data[] = null;
	public int readByte() {
		if(data == null) return 0;
		if(readPos >= data.length) return 0;
		int res = data[readPos];
		readPos++;
		res &= 0xff;
		return res;
	}
	public int readShort() {
		if(data == null) return 0;
		if(readPos + 1 >= data.length) return 0;
		int res = data[readPos];
		readPos++;
		int res2 = data[readPos];
		readPos++;
		res &= 0xff;
		res2 <<= 8;
		res |= res2;
		res &= 0xffff;
		return res;
	}
}
class ClientThread extends Thread{
	class Control2ClientThread extends Thread{
		private ClientThread cl;
		private int secssion;
		public Control2ClientThread(ClientThread cl,int secssion) {
			this.cl = cl;
			this.secssion = secssion;
		}
		public void run() {
			cl.control2Client(secssion);
		}
	}
	
	private Socket client;
	private int bindPort;
	private int maxLink;
	private InputStream clienti = null;
	private OutputStream cliento = null;
	public Socket control[] = new Socket[32];
	public OutputStream controlo[] = new OutputStream[32];
	public InputStream controli[] = new InputStream[32];
	private AtomicInteger clientWriteLock = new AtomicInteger(0);
	public volatile long prevTime;
	private ServerSocket sc = null;
	private AtomicInteger curLink = new AtomicInteger(0);
	private int version;
	
	Thread keepaliveThread = null;
	Thread control2ClientThread[] = new Thread[32];
	
	private AtomicInteger secssionMask = new AtomicInteger(0);
	
	private int allocSecssion() {
		if(curLink.get() >= maxLink) return -1;
		curLink.getAndIncrement();
		while(true) {
			int mask = secssionMask.get();
			for(int i = 0;i < 32;i++) if((mask & (1 << i)) == 0) {
				if(secssionMask.compareAndSet(mask, mask | (1 << i)) == true) return i;
			}
		}
	}
	private void freeSecssion(int secssion) {
		secssion &= 0x1f;
		while(true) {
			int mask = secssionMask.get();
			int mask2 = mask & ~(1 << secssion);
			if(secssionMask.compareAndSet(mask, mask2) == true) break;
		}
		curLink.decrementAndGet();
	}
	
	static int byte2Int(byte s[]) {
		if(s.length == 0) return 0;
		int s0 = 0;
		int s1 = 0;
		int s2 = 0;
		int s3 = 0;
		switch(s.length) {
		default:
		case 4:
			s3 = s[3];
		case 3:
			s2 = s[2];
		case 2:
			s1 = s[1];
		case 1:
			s0 = s[0];
		}
		s3 <<= 24;
		s2 <<= 16;
		s1 <<= 8;
		s0 &= 0xff;
		s1 &= 0xff00;
		s2 &= 0xff0000;
		s3 &= 0xff000000;
		return s0 | s1 | s2 | s3;
	}
	static void int2Byte(byte s[],int val) {
		if(s.length == 0) return;
		switch(s.length) {
		default:
		case 4:
			s[3] = (byte)(val >> 24);
		case 3:
			s[2] = (byte)(val >> 16);
		case 2:
			s[1] = (byte)(val >> 8);
		case 1:
			s[0] = (byte)val;
		}
	}
	
	public ClientThread(Socket sc) throws Exception{
		this.client = sc;
		clienti = sc.getInputStream();
		cliento = sc.getOutputStream();
	}
	private boolean readDataFromClient(byte buf[]) throws Exception{
		int cur = 0;
		while(cur < buf.length) {
			int ret = clienti.read(buf,cur,buf.length - cur);
			if(ret < 0) return false;
			cur += ret;
		}
		return true;
	}
	private Pack readPackFromClient() throws Exception {
		byte lenb[] = new byte[2];
		byte type[] = new byte[1];
		boolean ret;
		Pack res = new Pack();
		ret = readDataFromClient(lenb);
		if(ret == false) return null;
		int len = byte2Int(lenb);
		ret = readDataFromClient(type);
		if(ret == false) return null;
		res.type = type[0];
		res.type &= 0xff;
		if(len == 0) return res;
		res.data = new byte[len];
		ret = readDataFromClient(res.data);
		if(ret == false) return null;
		res.dataSize = len;
		return res;
	}
	private boolean writePackToClient(Pack p){
		try {
			byte total[] = new byte[3 + p.dataSize];
			int2Byte(total,p.dataSize);
			total[2] = (byte)(p.type);
			for(int i = 0;i < p.dataSize;i++) total[i + 3] = p.data[i];
			clientWriteLock.compareAndExchange(0, 1);
			if(client.isClosed() == true) {
				clientWriteLock.set(0);
				return false;
			}
			cliento.write(total,0,3 + p.dataSize);
			cliento.flush();
			clientWriteLock.set(0);
			return true;
		} catch (IOException e) {
			return false;
		}
	}
	public void throwCloseConnectPack(String reason) throws Exception {
		Pack cp = new Pack();
		cp.type = Pack.TYPE_DISCONNECT;
		byte data[] = reason.getBytes();
		cp.dataSize = data.length;
		cp.data = data;
		writePackToClient(cp);
	}
	private boolean handshake(){
		Pack hs;
		try {
			hs = readPackFromClient();
			if(hs.type != Pack.TYPE_HANDSHAKE || hs.dataSize < 2) {
				String reason = "Request a handshake pack, but recive a unknow type:" + hs.type + ".";
				throwCloseConnectPack(reason);
				System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
						"Recive unrecognice pack.");
				return false;
			}
			if(hs.dataSize == 2) {
				version = 0x0001;
				bindPort = hs.readShort();
				maxLink = 1;
			}
			else {
				if(hs.dataSize != 5) {
					String reason = "Bad handshake pack recive, request 2 or 5 bytes, but recive pack is(are) " +
							hs.dataSize + " byte(s).";
					throwCloseConnectPack(reason);
					System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
							"Recive bad handshake pack.");
					return false;
				}
				int majorVersion = hs.readByte();
				int minorVersion = hs.readByte();
				version = majorVersion << 8;
				version |= minorVersion;
				bindPort = hs.readShort();
				maxLink = hs.readByte();
				if(version != 0x0002) {
					String reason = "Unsupport TFRP version " + majorVersion + "." + minorVersion + ". Only support version equal or less than 0.2.";
					throwCloseConnectPack(reason);
					System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
							"Recive unsupport version pack.");
					return false;
				}
			}
			if(bindPort == 0) {
				String reason = "Can not bind port 0.";
				throwCloseConnectPack(reason);
				System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
						"Recive a handshake request to bind port 0.");
				return false;
			}
			if(maxLink > 32) {
				String reason = "Can not support maxLink more than 32.";
				throwCloseConnectPack(reason);
				System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
						"Recive a handshake try to set maxLink more than 32.");
				return false;
			}
			try {
				sc = new ServerSocket(bindPort);
			}catch(Exception e) {
				String reason = "Can not bind port:" + bindPort + ".";
				throwCloseConnectPack(reason);
				System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
						reason);
				return false;
			}
			writePackToClient(hs);
			System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + "Handshake success.");
			return true;
		} catch (Exception e) {
			System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + "Unknow error arise.");
			e.printStackTrace();
			return false;
		}
	}
	void closeSC() {
		try {
			sc.close();
		} catch (IOException e) {}
	}
	void closeClient() {
		try {
			client.shutdownInput();
			client.shutdownOutput();
			client.close();
		} catch (IOException e) {}
	}
	
	public void keepalive() {
		System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
				"Keepalive thread is running.");
		prevTime = System.currentTimeMillis();
		int loop = 0;
		Pack p = new Pack();
		p.type = Pack.TYPE_KEEPALIVE;
		try {
			while(true) {
				sleep(1000);
				if(System.currentTimeMillis() - prevTime >= tfrps.KEEPALIVE_THREHOLD) {
					System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
							"Connect out-of-time.");
					client.close();
					break;
				}
				if(loop++ >= 6) {//Send keepalive per 6-seconds
					loop = 0;
					if(writePackToClient(p) == false) break;
				}
			}
		} catch(Exception e) {}
		System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
				"Keepalive thread is stop.");
	}
	public void control2Client(int secssion) {
		System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
				"Control to client thread is running on secssion " + secssion + ".");
		byte data[];
		if(version == 0x0002) data = new byte[4097];
		else data = new byte[4096];
		
		Pack p = new Pack();
		p.data = data;
		p.type = Pack.TYPE_DATA;
		if(version == 0x0002) data[0] = (byte)secssion;
		
		Pack stopPack = new Pack();
		stopPack.type = Pack.TYPE_STOP_CONNECT;
		stopPack.data = data;
		if(version == 0x0002) stopPack.dataSize = 1;
		
		try {
			while(true) {
				int avilable;
				if(version == 0x0002) avilable = controli[secssion].read(data,1,4096);
				else avilable = controli[secssion].read(data);
				if(avilable < 0) break;
				p.dataSize = avilable;
				if(version == 0x0002) p.dataSize += 1;
				writePackToClient(p);
			}
		}catch(Exception e) {}
		try {
			writePackToClient(stopPack);
			System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
				"Secssion " + secssion + " is stop.");
			controli[secssion].close();
		} catch(Exception e3) {}
		System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
				"Control to client thread is stop on secssion " + secssion + ".");
	}
	private void listenThread() {
		while(true) {
			Socket preControl = null;
			try {
				preControl = sc.accept();
			} catch (Exception e) {
				closeSC();
				closeClient();
				tfrps.linkCur.decrementAndGet();
				return;
			}
			int secssion = allocSecssion();
			if(secssion < 0) {
				try {
					preControl.shutdownInput();
					preControl.shutdownOutput();
					preControl.close();
				}catch(Exception e2) {}
				continue;
			}
			secssion &= 0x1f;
			control[secssion] = preControl;
			try {
				controlo[secssion] = preControl.getOutputStream();
				controli[secssion] = preControl.getInputStream();
			} catch (IOException e) {
				try {
					preControl.shutdownInput();
					preControl.shutdownOutput();
					preControl.close();
				}catch(Exception e2) {}
				freeSecssion(secssion);
				continue;
			}
			System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
					"Recive a connect from" + preControl.getInetAddress().getHostAddress() + " as secssion " + secssion + ".");
			Pack scp = new Pack();
			scp.type = Pack.TYPE_START_CONNECT;
			scp.data = new byte[1];
			scp.data[0] = (byte)secssion;
			scp.dataSize = 1;
			writePackToClient(scp);
			control2ClientThread[secssion] = new Control2ClientThread(this,secssion);
			control2ClientThread[secssion].start();
		}
	}
	public void run() {
		if(handshake() == false) {
			closeClient();
			tfrps.linkCur.decrementAndGet();
			return;
		}
		secssionMask.set(0);
		keepaliveThread = new Thread(()->{keepalive();});
		keepaliveThread.start();
		new Thread(()->{listenThread();}).start();
		try {
			while(true) {
				Pack p = readPackFromClient();
				if(p == null) break;
				if(p.type == Pack.TYPE_KEEPALIVE) prevTime = System.currentTimeMillis();
				else if(p.type == Pack.TYPE_DATA) {
					int secssion = 0;
					if(version == 0x0002) {
						secssion = p.readByte() & 0x1f;
						controlo[secssion].write(p.data,1,p.dataSize - 1);
					}
					else controlo[0].write(p.data,0,p.dataSize);
				}
				else if(p.type == Pack.TYPE_DISCONNECT) {
					System.out.println("Connection from:" + client.getInetAddress().getHostAddress() + " be close by client." + new String(p.data));
					break;
				}
				else if(p.type == Pack.TYPE_STOP_CONNECT) {
					int secssion = 0;
					if(version == 0x0002) secssion = p.readByte() & 0x1f;
					System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
							"Secssion " + secssion + " is stop by client.");
					try {
						controlo[secssion].close();
						control[secssion].shutdownOutput();
						control[secssion].close();
						freeSecssion(secssion);
					} catch(Exception e) {}
					freeSecssion(secssion);
				}
				else {
					throwCloseConnectPack("Recive a unknow pack, type:" + p.type + ".");
					System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
							"Recive a unknow pack, type:" + p.type + ".");
					break;
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		closeSC();
		System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) +
				"Client disconnect.");
		for(int i = 0;i < 32;i++) {
			Thread t = control2ClientThread[i];
			if(t != null) t.interrupt();
		}
		keepaliveThread.interrupt();
		try {
			keepaliveThread.join();
			for(int i = 0;i < 32;i++) if(control2ClientThread[i] != null) control2ClientThread[i].join();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		try {
			client.shutdownInput();
			client.shutdownOutput();
			client.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
}

public class tfrps {
	public static final int DEF_BIND_PORT = 18086;
	public static final int DEF_MAX_LINK = 10;
	public static final long KEEPALIVE_THREHOLD = 1000*30;
	public static final int MAJOR_VERSION = 0;
	public static final int MINOR_VERSION = 2;
	public static AtomicInteger linkCur = new AtomicInteger(0);
	
	static void listenPort(int maxLink,int bindPort) throws Exception{
		ServerSocket ss = new ServerSocket(bindPort);
		Socket sc = null;
		System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
				"Start listen connection.");
		while(true) {
			sc = ss.accept();
			int cur = linkCur.get();
			InetAddress addr = sc.getInetAddress();
			System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
					"Got connect from:" + addr.getHostAddress());
			if(cur >= maxLink) {
				sc.close();
				System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
						"Link more than limit:" + maxLink + ",reject connection.");
				continue;
			}
			linkCur.addAndGet(1);
			try {
				new ClientThread(sc).start();
			} catch(Exception e) {
				System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
						"Create thread fail, close connection.");
				sc.close();
				e.printStackTrace();
			}
		}
	}
	public static void main(String argv[]) {
		//tfrps max_link bind_port
		int bindPort = DEF_BIND_PORT;
		int maxLink = DEF_MAX_LINK;
		if(argv.length >= 1) {
			maxLink = Integer.parseInt(argv[0]);
			if(maxLink == 0) maxLink = DEF_MAX_LINK;
			if(argv.length >= 2) {
				bindPort = Integer.parseInt(argv[1]);
				if(bindPort == 0) bindPort = DEF_BIND_PORT;
			}
		}
		
		System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
				"TFRPS: Max connect limit " + maxLink);
		System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis()) + 
				"TFRPS: Bind port " + bindPort);
		try {
			listenPort(maxLink,bindPort);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
