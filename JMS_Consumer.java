/*
 This program implements consumer for the JMS queue. It can be launched parallely on multiple consumers(machines). 
 */
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Topic;
import javax.jms.Destination;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.activemq.ActiveMQConnectionFactory;



public class Sonar {

 
  private String pbroker, pqueue, puser, ppass; // properties
  private String pfile  = "USS.properties";     
  private String shipName;

  public static void main(String[] args) throws Exception {

        if (args.length < 1) {
                System.err.println(
                        " Usage: java Sonar <shipname>  ## Consumer mode\n");
                System.exit(2);
        }
	Sonar c = new Sonar(args[0]);
	c.run();

  } // main

  public void loadprop(String filename) { // load properties to pbroker, pqueue etc
        
        try (InputStream input = new FileInputStream(filename)) {

        	Properties prop = new Properties();
        	prop.load(input);
        
        	pbroker = prop.getProperty("broker");
        	pqueue = prop.getProperty("queue");
        	puser = prop.getProperty("username");
        	ppass = prop.getProperty("password");
	} catch (IOException ex) {
		ex.printStackTrace();
	}
        
        
	  
  }

  public Sonar(String a) {

        loadprop(pfile);
        shipName = a;

  } 
  int run() throws javax.jms.JMSException
  {
        int n=0;
        Connection cx=null; Session sx=null;    
        ConnectionFactory cf=null;             

        try {
          cf = new ActiveMQConnectionFactory(pbroker);
          cx = cf.createConnection(puser, ppass);//puser is the username, ppass is the password
          sx = cx.createSession(false, Session.AUTO_ACKNOWLEDGE); 

          // make Destination, Consumer
          Destination destination = sx.createQueue(pqueue);     
          MessageConsumer messageConsumer= sx.createConsumer(destination);
          System.out.println("*DRILL *DRILL *DRILL USS "+shipName+" Sonar connected to: "+pbroker);

          // start consumer
          cx.start();
          System.out.println("*DRILL *DRILL *DRILL USS "+shipName+" at "+time_now()+": READY");

          while (true) {        
                TextMessage m = null;
		m = (TextMessage) messageConsumer.receive();
                if((m.getText()).equals("END"))
                {
                  break;
                }
                System.out.println(time_now()+" "+shipName+": "+m.getText());
              
              
          } // while
          System.out.println(time_now()+" "+shipName+": **TERMINATE DRILL");
         

          } catch( Exception e) {
                 e.printStackTrace();
          } finally {
                if(sx != null) sx.close();
                if(cx != null) cx.close();

          } 

          return n;
        } 

  String time_now() {
     
	  SimpleDateFormat date = new SimpleDateFormat("MM-dd-yyyy HH-mm-ss");//dd/MM/yyyy
	  Date now = new Date();
	  String strDate = date.format(now);
	  
	  return strDate;
	
  }


} 