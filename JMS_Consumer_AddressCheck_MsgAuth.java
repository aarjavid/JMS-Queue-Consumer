/*
This program implements consumer for the JMS queue. It can be launched parallely on multiple consumers(machines). 
The consumer recieves only the dedicated messages sent to itself identified by a tag. It does not recieve messages addressed for other consumers on the queue.
This program will also authenticate each recieved message to check if its real or fake by using java message properties. 
 */

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Topic;
import javax.jms.Destination;
import javax.jms.Session;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.QueueBrowser;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.naming.*;




public class SonarC {

 
  private String pbroker, pqueue, puser, ppass; // properties
  private String pfile  = "USS.properties";    
  private String shipName;

  public static void main(String[] args) throws Exception {

        if (args.length < 1) {
                System.err.println(
                        " Usage: java B <shipname>  ## Consumer mode\n");
                System.exit(2);
        }
	SonarC c = new SonarC(args[0]);
	c.run();

  } 

  public void loadprop(String filename) { 
       
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

  public SonarC(String a) {
        loadprop(pfile);
      
        shipName = a;

  }
  int run() throws javax.jms.JMSException
  {
        int n=0;
        Connection cx=null; Session sx=null;    
        ConnectionFactory cf=null;              
        MessageConsumer messageConsumer= null;
        MessageConsumer TermConsumer= null; 
     
        try {
          cf = new ActiveMQConnectionFactory(pbroker);
          cx = cf.createConnection(puser, ppass);//puser is the username, ppass is the password
          sx = cx.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         
          // make Destination, Consumer
          Destination destinationQ = sx.createQueue(pqueue);   
          Topic destinationT = sx.createTopic("TSONAR");
           messageConsumer = sx.createConsumer(destinationQ, "(boat='"+shipName+"')");
          TermConsumer = sx.createConsumer(destinationT); //this is the consumer for the term message
          System.out.println("*DRILL *DRILL *DRILL USS "+shipName+"  connected to: "+pbroker);

          // start consumer
          cx.start();
          System.out.println("*DRILL *DRILL *DRILL USS "+shipName+" at "+time_now()+": READY");
          TextMessage m = null;
          TextMessage t = null;
          Auth myKey = new Auth("AABECZT");
          Message am = null;
          String signCheck = null;
          String signM = null;
         
          while (true) {       
                am = messageConsumer.receiveNoWait();
                if(am!=null){
                  signCheck = am.getStringProperty("signature");
                  m = (TextMessage) am;
                  signM = myKey.sign(m.getText());
                  System.out.print(time_now()+" "+shipName+": "+m.getText());
                  if(signM.equals(signCheck))
                     System.out.println(" (Authentic)");
                  else
                     System.out.println(" (NOT AUTHENTIC)");
                  m = null; am = null; signM=null; signCheck=null;
                }
                else{
  		            am = TermConsumer.receiveNoWait();
                  if(am!=null){
                    signCheck = am.getStringProperty("signature");
                    t = (TextMessage) am;
                    signM = myKey.sign(t.getText());
                    System.out.print(time_now()+" "+shipName+": "+t.getText());
                    if(signM.equals(signCheck))
                    {
                      System.out.println(" (Authentic)");
                      if(t.getText().equals("END"))
                        break;
                    }
                    else
                    {
                      System.out.println(" (NOT AUTHENTIC)");
                    }
                    t=null; signM=null; signCheck=null; am=null;
                  }
                }
            Thread.sleep(400);
          } 
          System.out.println(time_now()+" "+shipName+": **TERMINATE DRILL");
         
          } catch( Exception e) {
                 e.printStackTrace();
          } finally {
                if(messageConsumer !=null) messageConsumer.close();
                if(TermConsumer !=null) TermConsumer.close();
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