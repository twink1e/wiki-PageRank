import java.io.*;

class DataProcessor {
  private static int getLittleEndian(byte[] b) {
    return ((b[3]&0xff)<<24)+((b[2]&0xff)<<16)+((b[1]&0xff)<<8)+(b[0]&0xff);
  }

  private static void indexPages() {
    File in = new File("indexbi.bin");
    File out = new File("indexed.bin");
    try {
      FileInputStream fis = new FileInputStream(in);
      DataInputStream dis = new DataInputStream(fis);
      FileOutputStream fos = new FileOutputStream(out);
      DataOutputStream dos = new DataOutputStream(fos);
      byte[] buffer = new byte[4];
      int num;
      int count = 0;
      while (dis.available() > 0) {
        dis.read(buffer);
        num = getLittleEndian(buffer);
        if (num == 0) {
          num = count++;
          System.out.println(count);
        }
        dos.writeInt(num);
        //System.out.println(num);
      }
      dis.close();
      dos.close();
      if (count == 8166507) System.out.println("Success!");
      else System.out.println("Fail! " + count);
    } catch (Exception e) {
      System.out.println(e);
    }

  }

  private static void readBinary(String file, int num) {
    File in = new File(file);
    try {
      FileInputStream fis = new FileInputStream(in);
      DataInputStream dis = new DataInputStream(fis);
      for (int i=0; i<num; i++) dis.readInt();//System.out.println(dis.readInt());
      dis.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  private static void readBinaryBuffered(String file, int num) {
    try {
      FileInputStream in = new FileInputStream(file);
      BufferedInputStream fis = new BufferedInputStream(in);
      DataInputStream dis = new DataInputStream(fis);
      for (int i=0; i<num; i++) dis.readInt(); //System.out.println(dis.readInt());
      dis.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  public static void main (String[] args) {
    //indexPages();
    long start = System.currentTimeMillis();
    readBinary("indexbi.bin", 10000);
    long end = System.currentTimeMillis();
    System.out.println("took "+ (end-start));
    start = end;
    readBinaryBuffered("indexbi.bin", 10000);
    end = System.currentTimeMillis();
    System.out.println("buffered took "+ (end-start));
  }
}