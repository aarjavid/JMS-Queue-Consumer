/*
 This program implements consumer for the JMS queue. It can be launched parallely on multiple consumers(machines). 
 The consumer recieves only the dedicated messages sent to itself identified by a tag. It does not recieve messages addressed for other consumers on the queue.
 */
 
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Topic;
import javax.jms.Destination;
import javax.jms.Session;
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




public class SonarB {

 
  private String pbroker, pqueue, puser, ppass; 
  private String pfile  = "USS.properties";    
  private String shipName;

  public static void main(String[] args) throws Exception {

        if (args.length < 1) {
                System.err.println(
                        " Usage: java B <shipname>  ## Consumer mode\n");
                System.exit(2);
        }
	SonarB c = new SonarB(args[0]);
	c.run();

  } 

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

  public SonarB(String a) {
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
         
          while (true) {        /
               
                m = (TextMessage) messageConsumer.receiveNoWait();
                if(m!=null){
                  System.out.println(time_now()+" "+shipName+": "+m.getText());
                  m = null;
                }
                else{
                  //This is Topic
  		            t = (TextMessage) TermConsumer.receiveNoWait();
                  if(t!=null){
                    System.out.println(time_now()+" "+shipName+": "+t.getText());
                    break;
                  }
                }
            Thread.sleep(400);
          } // while
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
  
} // class