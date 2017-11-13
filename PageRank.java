import java.io.*;
import java.nio.*;
import java.nio.channels.*;

class PageRank {
  private static final String LINK = "links.bin", SRC = "src.bin", DST = "dst.bin";
  private static final double DAMPING = 0.85;
  private static int numPage;

  private static void pageRankIter(int srcMem, int linkMem, int dstMem) {
    try {
      FileChannel in = new FileInputStream(LINK).getChannel();
      MappedByteBuffer mbb = in.map(FileChannel.MapMode.READ_ONLY, 0, linkMem>in.size()?in.size():linkMem);
      while(mbb.hasRemaining()) mbb.getInt();
      mbb.position((int)in.size()-4);
      System.out.println(in.size());
      System.out.println(mbb.getInt());
      in.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  private static void initSrc() {
    try {
      FileInputStream fis = new FileInputStream(LINK);
      BufferedInputStream bis = new BufferedInputStream(fis);
      DataInputStream dis = new DataInputStream(bis);

      FileOutputStream fos = new FileOutputStream(SRC);
      BufferedOutputStream bos = new BufferedOutputStream(fos);
      DataOutputStream dos = new DataOutputStream(bos);

      numPage = dis.readInt();
      dis.close();
      double score = 1.0/numPage;
      for(int i=0; i<numPage; i++) dos.writeDouble(score);
      dos.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  public static void main (String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: java PageRank {iteration} {src MB} {link MB} {dst MB}");
      System.exit(-1);
    }
    int iter = Integer.parseInt(args[0]);
    int srcMem = Integer.parseInt(args[1]);
    int linkMem = Integer.parseInt(args[2]);
    int dstMem = Integer.parseInt(args[3]);
    long start, end, iterStart, iterEnd;
    start = System.currentTimeMillis();
    initSrc();
    end = System.currentTimeMillis();
    System.out.println("Init src took "+ (end - start) + " ms");
    start = System.currentTimeMillis();
    for (int i=0; i<iter; i++) {
      iterStart = System.currentTimeMillis();
      pageRankIter(srcMem * 1000000, linkMem * 1000000, dstMem * 1000000);
      iterEnd = System.currentTimeMillis();
      System.out.println("Iter " + i + " took "+ (iterEnd - iterStart) + " ms");
    }
    end = System.currentTimeMillis();
    System.out.println(iter + " iter " + srcMem + " MB src " + linkMem + " MB link " + dstMem + " MB dst took " + (end - start) + " ms");
  }
}