package glx.cn.sh;
import glx.cn.sh.TestData.*;
/**
 *
 * protobuf functions
 *
**/

public class protoHelper
{
  public  protoHelper(){}

  public interface pbSerializer{
    public void initial(byte[] bytes) throws Exception; 
    public String getDspName();
//    public void setDspName(String name);
//    public int getRtbId();
  }

  public interface pbDeserializer{
    public void initial(Object obj, Class cls);
    public String getDspName();
    public void setDspName(String name);
    public int getRtbId();
  }

  public interface pbFactory{
    public pbSerializer getSerializer();
    public pbDeserializer getDeserializer();

  }

  public class tdSerializer implements pbSerializer{
    LogGroup lg;
    public void initial(byte[] bytes) throws Exception{
      lg = LogGroup.parseFrom(bytes);
    }

    public String getDspName(){
      for (TestData.Site site : lg.getSiteList()){
        return site.getName();
      }
      return null;
    }
  }
}
