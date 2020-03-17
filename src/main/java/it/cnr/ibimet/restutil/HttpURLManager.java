package it.cnr.ibimet.restutil;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;


import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;


import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

public class HttpURLManager {
    private final String USER_AGENT="Mozilla/5.0";

    private String url;


    public HttpURLManager() {
        super();

    }

    public HttpURLManager(String url){
        this();
        this.url=url;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String sendGet() throws Exception{
        URL obj = new URL(url);
        HttpURLConnection con =(HttpURLConnection)obj.openConnection();


        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");

        int responseCode=con.getResponseCode();
        System.out.println("Call service: "+url);
        System.out.println("RetCode: "+responseCode);

        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));

        String inputLine;
        StringBuffer response = new StringBuffer();

        while((inputLine=in.readLine())!=null){
            response.append(inputLine);
        }


        in.close();

        System.out.println(response.toString());

        return(response.toString());
    }
   

}
